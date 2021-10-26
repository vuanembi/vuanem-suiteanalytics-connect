import os
from datetime import datetime

import jinja2
from google.cloud import bigquery

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

NOW = datetime.utcnow()
DATASET = "NetSuite"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

ROWS_PER_FETCH = 50000

BQ_CLIENT = bigquery.Client()
MAX_LOAD_ATTEMPTS = 2

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

QUERIES_LOADER = jinja2.FileSystemLoader(searchpath="./queries")
QUERIES_ENV = jinja2.Environment(loader=QUERIES_LOADER)

def get_engine():
    return create_engine(
        URL.create(
            drivername="postgresql+psycopg2",
            username=os.getenv("PG_UID"),
            password=os.getenv("PG_PWD"),
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DB"),
        ),
        executemany_mode="values",
        executemany_values_page_size=1000,
    )
