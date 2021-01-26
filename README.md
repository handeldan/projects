This is the script that extracts, transforms, and loads data from Postgres to BigQuery



###### The following general environmental variables are required:
* CONFIG_FILE: The path to the configuration file
* GOOGLE_APPLICATION_CREDENTIALS: The path to the gcloud credentials file
* EXTRACTION_DIR: Working directory for extracted files
* LOG_LEVEL: NOTSET, DEBUG, INFO, WARNING, ERROR, CRITICAL
###### The following secrets also need to be exposed as environmental variables:
* REDIS_PASSWORD
* POSTGRES_USER
* POSTGRES_PASSWORD