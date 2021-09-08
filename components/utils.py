import os
from datetime import datetime

import jinja2
from google.cloud import bigquery

from sqlalchemy import create_engine

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

ENGINE = create_engine(
    "postgresql+psycopg2://"
    + f"{os.getenv('PG_UID')}:{os.getenv('PG_PWD')}@"
    + f"{os.getenv('PG_HOST')}/{os.getenv('PG_DB')}",
    executemany_mode="values",
    executemany_values_page_size=10000,
    # echo=True,
)
