import os
import sys
import json
import csv
import uuid
import queue
import threading
import datetime
import logging
from concurrent.futures import TimeoutError

import redis
import slack
from retry import retry
import psycopg2 as pg
from google.cloud import bigquery
from google.cloud import exceptions as gcloud_exceptions

# Set the initial log level to DEBUG
logging.basicConfig(
    level=logging.getLevelName(os.environ["LOG_LEVEL"]),
    format="%(asctime)s %(levelname)s %(message)s",
)
config = None
redis_connection_pool = None
# Prefix
prefix = ""
# Date column
date_column = ""
# Errors to be sent to slack
errors_queue = queue.Queue()
# Increase csv.field_size_limit
csv_size_limit = csv.field_size_limit()
csv.field_size_limit(csv_size_limit * 5)


def get_config():
    """Read configuration from json config file and return a python object"""
    with open(os.environ["CONFIG_FILE"], "r") as config:
        return json.load(config)


def get_last_etl_timestamp(database, table):
    """Get the timestamp for when table was last extracted and loaded"""
    r = redis.Redis(connection_pool=redis_connection_pool)
    return r.get(f"etl_time_stamp.{prefix}{database}.{table}")


def set_next_etl_timestamp(database, table, time):
    """Set the timestamp after a successfull extraction and loading"""
    r = redis.Redis(connection_pool=redis_connection_pool)
    r.set(f"etl_time_stamp.{prefix}{database}.{table}", time)


class ETLLockError(Exception):
    """Exception raised when acquiring the ETL lock fails"""

    pass


class ETLLock:
    """Context manager for ETL locks"""

    def __enter__(self):
        r = redis.Redis(connection_pool=redis_connection_pool)
        self.uuid = uuid.uuid4().hex
        lock_state = r.get(f"{prefix}etl_extraction_locked")
        if lock_state is None:
            logging.info(f"Trying to acquire lock with UUID: {self.uuid}")
            r.set(
                f"{prefix}etl_extraction_locked", self.uuid, ex=3600, nx=True
            )
            validation_uuid = r.get(f"{prefix}etl_extraction_locked")
            logging.info(f"Found validation UUID: {validation_uuid}")
            if validation_uuid == self.uuid:
                logging.info("UUIDs match, proceeding")
                return self
            else:
                raise ETLLockError()
        else:
            raise ETLLockError()

    def __exit__(self, exception_type, exception_value, traceback):
        r = redis.Redis(connection_pool=redis_connection_pool)
        lock_state = r.get(f"{prefix}etl_extraction_locked")
        if lock_state == self.uuid:
            logging.info(f"Giving up lock with UUID: {self.uuid}")
            r.delete(f"{prefix}etl_extraction_locked")
        else:
            raise ETLLockError()


class DatabaseConnection:
    """Context manager for database connections"""

    def __init__(self, dbname, user, password):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection = pg.connect(
            dbname=self.dbname,
            user=self.user,
            password=self.password,
            host=config["postgres"]["host"],
            port=config["postgres"]["port"],
        )

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.connection.close()


def is_csv_empty(path):
    """Checks whether a CSV file contains at least one row, minus the header"""
    with open(path, "r") as csv:
        if csv.readline() and csv.readline():
            return False
        else:
            return True


def map_type(postgres_type):
    """Map a Postgres type to a BQ type, the default being STRING"""
    return {
        "boolean": "BOOL",
        "timestamp without time zone": "TIMESTAMP",
        "time without time zone": "TIME",
        "timestamp with time zone": "TIMESTAMP",
        "time with time zone": "TIME",
        "integer": "INTEGER",
        "date": "date",
        "double precision": "FLOAT",
        "numeric": "FLOAT",
        "smallint": "INTEGER",
        "bignint": "INTEGER",
        "bytea": "BYTES",
        "real": "FLOAT",
        "serial": "INTEGER",
        "smallserial": "INTEGER",
    }.get(postgres_type, "STRING")


@retry(tries=3, delay=2, backoff=2)
def create_view(database, table, view_column, columns):
    client = bigquery.Client(project="twigadms", location="US")
    select_columns = ",".join(
        map(lambda column: f"original.{column} as {column}", columns)
    )
    view_query = (
        f"SELECT DISTINCT {select_columns} "
        f"FROM twigadms.dmslive.{prefix}{database}_{table} original, "
        f"(SELECT DISTINCT {view_column}, MAX({date_column}) AS {date_column} "
        f"FROM twigadms.dmslive.{prefix}{database}_{table} GROUP BY {view_column}) latest "
        f"WHERE latest.{view_column} = original.{view_column} AND latest.{date_column} = original.{date_column}"
    )
    query = (
        f"CREATE OR REPLACE VIEW `twigadms.dmslive.views_{prefix}{database}_{table}` "
        f'OPTIONS (description="This view deduplicates rows by using max({date_column})") '
        f"AS {view_query}"
    )
    job = client.query(query)
    try:
        job.result(timeout=60 * 5)
    except gcloud_exceptions.GoogleCloudError as err:
        logging.error(
            f'Encountered error creating view "twigadms.dmslive.views_{prefix}{database}_{table}": {err}\n{job.error_result}. Errors: {job.errors}'
        )
    except TimeoutError:
        logging.error(
            f'Timed out creating view "twigadms.dmslive.views_{prefix}{database}_{table}"'
        )
    else:
        logging.info(
            f'Created new view "{job.destination.project}.{job.destination.dataset_id}.{job.destination.table_id}"'
        )


class ETLJob:
    """This class encapsulates the extraction and loading for individual tables and also acts as a context manager"""

    def __init__(self, database, table, dataset, db_connection):
        self.db_connection = db_connection
        self.database = database
        self.dataset = dataset
        self.cursor = self.db_connection.cursor()
        self.table = table
        self.path = (
            os.environ["EXTRACTION_DIR"] + f"/{prefix}{database}.{table}"
        )
        self.next_etl_timestamp = datetime.datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.last_etl_timestamp = get_last_etl_timestamp(database, table)
        self.fields = []
        self.succeeded = False

    def __enter__(self):
        self.cursor.execute(
            f"select column_name, data_type from information_schema.columns where table_name='{self.table}';"
        )
        while True:
            row = self.cursor.fetchone()
            if row is None:
                break
            else:
                self.fields.append(row)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.succeeded:
            set_next_etl_timestamp(
                self.database, self.table, self.next_etl_timestamp
            )

    def get_schema(self):
        """Query schema from Postres and return a list"""
        schema = []
        for row in self.fields:
            if row is None:
                break
            else:
                if row[0] not in config["skip_columns"]:
                    schema.append(
                        bigquery.SchemaField(
                            row[0], map_type(row[1]), mode="NULLABLE"
                        )
                    )
        return schema

    def find_unique_non_null(self):
        query = """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = '{}'::regclass
        AND i.indisunique
        AND a.attnotnull;
        """.format(
            self.table
        )
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row is None:
            return None
        else:
            return row[0]

    def get_primary_key(self):
        query = """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
        AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = '{}'::regclass
        AND    i.indisprimary;
        """.format(
            self.table
        )
        self.cursor.execute(query)
        row = self.cursor.fetchone()
        if row is None:
            return None
        else:
            return row[0]

    def get_view_column(self):
        return self.get_primary_key() or self.find_unique_non_null()

    def has_date_column(self):
        for row in self.fields:
            if row[0] == date_column:
                return True
        return False

    def get_columns_array(self):
        allowed_columns = []
        for row in self.fields:
            if row[0] not in config["skip_columns"]:
                allowed_columns.append(row[0])
        return allowed_columns

    def get_columns(self):
        return ",".join(self.get_columns_array())

    def extract(self):
        """Use COPY to extract data into a file"""
        with open(
            self.path, mode="w+", encoding="utf-8", newline=""
        ) as outfile:
            if self.last_etl_timestamp is None:
                query = f"COPY (SELECT {self.get_columns()} FROM public.{self.table}) TO STDOUT WITH CSV HEADER;"
                self.cursor.copy_expert(query, outfile)
            else:
                query = (
                    f"COPY (SELECT {self.get_columns()} FROM public.{self.table} WHERE"
                    f" {date_column} > to_timestamp('{self.last_etl_timestamp}', 'YYYY-MM-DD HH24:MI:SS'))"
                    f" TO STDOUT WITH CSV HEADER;"
                )
                if self.has_date_column():
                    self.cursor.copy_expert(query, outfile)
                else:
                    logging.info(
                        f'Skipping DB {prefix}{self.database} table {self.table} because it doesn\'t have column "{date_column}"'
                    )

    def transform(self):
        """BQ's CSV loading is brittle, clean up the CSV before calling load()"""
        with open(self.path, mode="r", newline="") as inputfile, open(
            self.path + "_transform", mode="w+", newline=""
        ) as outputfile:
            reader = csv.reader(
                inputfile,
                quoting=csv.QUOTE_MINIMAL,
                doublequote=True,
                lineterminator="\n",
                strict=True,
            )
            writer = csv.writer(
                outputfile,
                quoting=csv.QUOTE_MINIMAL,
                doublequote=True,
                lineterminator="\n",
                strict=True,
            )
            for row in reader:
                transformed = [col.replace("\r\n", r"\n") for col in row]
                transformed = [col.replace("\n", r"\n") for col in transformed]
                writer.writerow(transformed)
        os.rename(self.path + "_transform", self.path)

    @retry(tries=3, delay=2, backoff=2)
    def load(self):
        """Load data from the file written to by extract()"""
        client = bigquery.Client(project="twigadms", location="US")
        dataset_ref = client.dataset(self.dataset)
        table_ref = dataset_ref.table(f"{prefix}{self.database}_{self.table}")

        if is_csv_empty(self.path):
            logging.info(
                f"Nothing new to load for DB {prefix}{self.database} table {self.table}"
            )

            # Create the table and view if it doesn't exist
            if self.last_etl_timestamp is None:
                table = bigquery.table.Table(table_ref, self.get_schema())
                if client.create_table(table, exists_ok=True) is not None:
                    logging.info(
                        f"Created table {self.table} in DB {prefix}{self.database}"
                    )
                    self.succeeded = True

                view_column = self.get_view_column()
                if view_column is not None and self.has_date_column():
                    create_view(
                        self.database,
                        self.table,
                        view_column,
                        self.get_columns_array(),
                    )

            return

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            quote_character='"',
            skip_leading_rows=1,
            allow_quoted_newlined=True,
            schema=self.get_schema(),
            schema_update_options=None
            if self.last_etl_timestamp is None
            else [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
            if self.last_etl_timestamp is None
            else bigquery.WriteDisposition.WRITE_APPEND,
        )
        with open(self.path, "rb") as source_file:
            job = client.load_table_from_file(
                source_file, table_ref, job_config=job_config
            )
            # API request
            try:
                job.result(timeout=60 * 5)
            except gcloud_exceptions.GoogleCloudError:
                logging.error(
                    f"GoogleCloudError while loading from DB {prefix}{self.database} table {self.table} : {job.error_result}. Errors: {job.errors}"
                )
            except TimeoutError:
                logging.error(
                    f"Timed out loading from DB {prefix}{self.database} table {self.table}"
                )
            else:
                logging.info(
                    f"Loaded {job.output_rows} rows from {prefix}{self.database}.{self.table} into {self.dataset}:{prefix}{self.database}_{self.table}"
                )
                if self.last_etl_timestamp is None:
                    # Only try to create view the first time we load
                    view_column = self.get_view_column()
                    if view_column is not None and self.has_date_column():
                        create_view(
                            self.database,
                            self.table,
                            view_column,
                            self.get_columns_array(),
                        )
                    else:
                        logging.info(
                            f'Skipped creating view "view_{prefix}{self.database}_{self.table}": could not get primary key'
                        )


def job_worker(jobs_queue):
    """Get jobs from the queue until it is empty"""
    while True:
        try:
            job = jobs_queue.get_nowait()
        except queue.Empty:
            logging.debug("Queue is empty, exiting")
            break
        else:
            database = job[0]
            table = job[1]
            dataset = config["dataset"]
            logging.info(
                f"Starting job for DB {prefix}{database} table {table}"
            )
            try:
                with DatabaseConnection(
                    dbname=database,
                    user=os.environ["POSTGRES_USER"],
                    password=os.environ["POSTGRES_PASSWORD"],
                ) as conn:
                    with ETLJob(
                        database, table, dataset, conn.connection
                    ) as job:
                        try:
                            job.extract()
                            job.transform()
                        except Exception as err:
                            logging.error(
                                f"Uncaught exception extracting from DB {job.database} table {job.table}: {err}"
                            )
                            errors_queue.put(
                                {
                                    "database": job.database,
                                    "table": job.table,
                                    "stage": "extraction",
                                    "error": err,
                                }
                            )
                        else:
                            try:
                                job.load()
                            except Exception as err:
                                logging.error(
                                    f"Uncaught exception loading from DB {job.database} table {job.table}: {err}"
                                )
                                errors_queue.put(
                                    {
                                        "database": job.database,
                                        "table": job.table,
                                        "stage": "loading",
                                        "error": err,
                                    }
                                )
                            else:
                                job.succeeded = True
            except Exception as err:
                logging.error(f"Uncaught exception while running job: {err}")
            finally:  # Make sure we mark the task as done
                jobs_queue.task_done()


def run():
    """Read configuration and start jobs"""
    global redis_connection_pool, config, prefix, date_column
    config = get_config()
    if config["prefix"]:
        prefix = config["prefix"] + "_"
    date_column = config["date_column"]
    redis_connection_pool = redis.ConnectionPool(
        host=os.environ["REDIS_HOST"],
        port=os.environ["REDIS_PORT"],
        password=os.environ["REDIS_PASSWORD"],
        encoding="utf-8",
        decode_responses=True,
    )
    try:
        with ETLLock() as _:
            try:
                jobs_queue = queue.Queue()
                threads = []
                for database, tables in config["sources"].items():
                    for table in tables:
                        jobs_queue.put((database, table, config["dataset"]))
                for i in range(config["threads"]):
                    thread = threading.Thread(
                        target=job_worker, args=(jobs_queue,), daemon=True
                    )
                    threads.append(thread)
                    thread.start()
                jobs_queue.join()
                [t.join() for t in threads]
            except Exception as err:
                logging.error(f"Uncaught exception in run(): {err}")
                sys.exit(1)
    except ETLLockError:
        logging.info("Failed to acquire lock, quitting")


def notify_slack():
    client = slack.WebClient(token=os.environ["SLACK_API_TOKEN"])
    if errors_queue.empty():
        # Say nothing
        return
    else:
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "The following jobs encountered errors:",
                },
            }
        ]
        while True:
            try:
                message = errors_queue.get_nowait()
                blocks.append({"type": "divider"})
                blocks.append(
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*Database*: {prefix}"
                            "{database}\n*Table*: {table}\n*stage*:{stage}\n*Error*:```{error}```".format_map(
                                message
                            ),
                        },
                    }
                )
            except queue.Empty:
                break
        client.chat_postMessage(channel="datawarehousing", blocks=blocks)


try:
    run()
    notify_slack()
except Exception as err:
    logging.error(f"Uncaught exception while starting ETL script: {err}")
    sys.exit(1)
