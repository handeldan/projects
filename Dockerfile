FROM twigatech/alpine-base:v2

# alpine dependencies
RUN apk update
RUN apk add python3-dev build-base postgresql-dev

# Add non-root user
RUN adduser --home /opt/etl --disabled-password etl
USER etl
WORKDIR /opt/etl/

COPY etl.py /opt/etl/
COPY config.json /opt/etl/
COPY requirements.txt /opt/etl

# update pip
RUN python3 -m pip install --user --upgrade pip
RUN python3 -m pip install --user --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/etl/files

ENTRYPOINT python3.6 /opt/etl/etl.py
