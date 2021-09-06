import os
import json
import time
import io
from datetime import datetime
from abc import ABCMeta, abstractmethod

import jinja2
import jaydebeapi
from google.cloud import bigquery
from google.api_core.exceptions import Forbidden, NotFound

# from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from pg_models import Base

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
    # executemany_batch_page_size=20000,
    # echo=True,
)

import cProfile
import io
import pstats
import contextlib


@contextlib.contextmanager
def profiled():
    pr = cProfile.Profile()
    pr.enable()
    yield
    pr.disable()
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats()
    # uncomment this to see who's calling what
    # ps.print_callers()
    print(s.getvalue())


class JDBCConnector(metaclass=ABCMeta):
    account_id = os.getenv("ACCOUNT_ID")

    @property
    @abstractmethod
    def role_id(self):
        pass

    @property
    @abstractmethod
    def user(self):
        pass

    @property
    @abstractmethod
    def pwd(self):
        pass

    def connect(self):
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                f"jdbc:ns://{self.account_id}.connect.api.netsuite.com:1708;"
                f"ServerDataSource={self.data_source}.com;"
                "Encrypted=1;"
                f"CustomProperties=(AccountID={self.account_id};RoleID={self.role_id})"
            ),
            {
                "user": self.user,
                "password": self.pwd,
            },
            "NQjc.jar",
        )


class NetSuiteConnector(JDBCConnector):
    data_source = "NetSuite"
    role_id = os.getenv("ROLE_ID")
    user = os.getenv("NS_UID")
    pwd = os.getenv("NS_PWD")


class NetSuite2Connector(JDBCConnector):
    data_source = "NetSuite2"
    role_id = os.getenv("ROLE_ID2")
    user = os.getenv("NS_UID2")
    pwd = os.getenv("NS_PWD2")


class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.connector = model._connector
        data_source = model._connector.data_source
        self.table = model.table
        self.template = QUERIES_ENV.get_template(f"{data_source}/{model.table}.sql.j2")

    def get(self):
        with self.connector.connect() as conn:
            with conn.cursor() as cursor:
                query = self._build_query()
                cursor.execute(query)
                columns = [column[0] for column in cursor.description]
                rows = []
                while True:
                    results = cursor.fetchmany(ROWS_PER_FETCH)
                    if results:
                        rows.extend([dict(zip(columns, result)) for result in results])
                    else:
                        break
        return rows

    @abstractmethod
    def _build_query(self):
        pass

    @abstractmethod
    def _get_time_range(self):
        pass


class StandardGetter(Getter):
    def __init__(self, model):
        super().__init__(model)

    def _build_query(self):
        return self.template.render()

    def _get_time_range(self):
        pass


class IncrementalGetter(Getter):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys
        self.start, self.end = self._get_time_range(model.start, model.end)

    def _get_start(self):
        template = TEMPLATE_ENV.get_template(f"read_max_incremental.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            incre_key=self.keys["incre_key"],
        )
        try:
            rows = BQ_CLIENT.query(rendered_query).result()
            row = [row for row in rows][0]
            start = row["incre"]
        except NotFound:
            start = datetime(2018, 6, 30)
        return start

    def _build_query(self):
        return self.template.render(
            start=self.start,
            end=self.end,
        )


class TimeIncrementalGetter(IncrementalGetter):
    def __init__(self, model):
        super().__init__(model)

    def _get_time_range(self, start, end):
        if start and end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [start, end]
            ]
        else:
            end = NOW.strftime(TIMESTAMP_FORMAT)
            start = self._get_start().strftime(TIMESTAMP_FORMAT)
        return start, end


class IDIncrementalGetter(IncrementalGetter):
    def __init__(self, model):
        super().__init__(model)

    def _get_time_range(self, start, end):
        if start and end:
            start, end = start, end
        else:
            end = 50e7
            start = self._get_start()
        return start, end


class Loader(metaclass=ABCMeta):
    @property
    @abstractmethod
    def load(self):
        pass


class BigQueryLoader(Loader):
    def __init__(self, model):
        self.table = model.table
        self.schema = model.schema

    @property
    @abstractmethod
    def write_disposition(self):
        pass

    @property
    @abstractmethod
    def load_target(self):
        pass

    def load(self, rows):
        attempts = 0
        while True:
            try:
                loads = BQ_CLIENT.load_table_from_json(
                    rows,
                    f"{DATASET}.{self.load_target}",
                    job_config=bigquery.LoadJobConfig(
                        schema=self.schema,
                        create_disposition="CREATE_IF_NEEDED",
                        write_disposition=self.write_disposition,
                    ),
                ).result()
                break
            except Forbidden as e:
                if attempts < MAX_LOAD_ATTEMPTS:
                    time.sleep(30)
                    attempts += 1
                else:
                    raise e
        print(datetime.now().isoformat())
        return {
            "load": "BigQuery",
            "output_rows": loads.output_rows,
        }

    @abstractmethod
    def _update(self):
        pass


class BigQueryStandardLoader(BigQueryLoader):
    write_disposition = "WRITE_TRUNCATE"

    @property
    def load_target(self):
        return self.table

    def _update(self):
        pass


class BigQueryIncrementalLoader(BigQueryLoader):
    write_disposition = "WRITE_APPEND"

    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys

    @property
    def load_target(self):
        return f"_stage_{self.table}"

    def _update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=self.keys["p_key"],
            rank_key=self.keys["rank_key"],
            row_num_incre_key=self.keys["row_num_incre_key"],
            rank_incre_key=self.keys["rank_incre_key"],
        )
        BQ_CLIENT.query(rendered_query)


class PostgresLoader(Loader):
    def __init__(self, model):
        self.model = model.model

    def load(self, rows):
        with ENGINE.begin() as conn:
            loads = self._load(conn, rows)

        print(datetime.now().isoformat())
        return {
            "load": "Postgres",
            "output_rows": len(loads.inserted_primary_key_rows),
        }
    
    @abstractmethod
    def _load(self, conn, rows):
        pass

class PostgresStandardLoader(PostgresLoader):
    def _load(self, conn, rows):
        self.model.main.drop(bind=ENGINE, checkfirst=True)
        self.model.main.create(bind=ENGINE, checkfirst=True)
        loads = conn.execute(insert(self.model.main), rows)
        return loads


class PostgresIncrementalLoader(PostgresLoader):
    def _load(self, conn, rows):
        self.model.stage.drop(bind=ENGINE, checkfirst=True)
        self.model.stage.create(bind=ENGINE, checkfirst=True)
        loads = conn.execute(insert(self.model.stage), rows)
        self._update(conn)
        return loads

    def _update(self, conn):
        self.model.main.create(bind=ENGINE, checkfirst=True)
        stmt = insert(self.model.main).from_select(
            self.model.main.c, select(self.model.stage)
        )
        update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}
        stmt = stmt.on_conflict_do_update(
            index_elements=self.model.main.primary_key.columns,
            set_=update_dict,
        )
        conn.execute(stmt)
        self.model.stage.drop(bind=ENGINE, checkfirst=True)


class NetSuite(metaclass=ABCMeta):
    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def connector(self):
        pass

    @property
    @abstractmethod
    def getter(self):
        pass

    @property
    @abstractmethod
    def loader(self):
        pass

    @property
    def config(self):
        with open(f"configs/{self._connector.data_source}/{self.table}.json") as f:
            config = json.load(f)
        return config

    @property
    def schema(self):
        return self.config["schema"]

    def __init__(self, start, end):
        self.start, self.end = start, end
        self._connector = self.connector()
        self._getter = self.getter(self)
        self._loader = [loader(self) for loader in self.loader]

    def _transform(self, rows):
        int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]
        for row in rows:
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def run(self):
        rows = self._getter.get()
        print(datetime.now().isoformat())
        response = {
            "table": self.table,
            "data_source": self._connector.data_source,
            "num_processed": len(rows),
        }
        if getattr(self._getter, "start", None) and getattr(self._getter, "end", None):
            response["start"] = self._getter.start
            response["end"] = self._getter.end
        if len(rows) > 0:
            rows = self._transform(rows)
            print(datetime.now().isoformat())
            response["loads"] = [loader.load(rows) for loader in self._loader]
        return response
