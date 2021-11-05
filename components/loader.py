import time
from abc import ABCMeta, abstractmethod
import io
import csv
import os

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Identity,
    Integer,
    delete,
    select,
    func,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import URL

from .utils import BQ_CLIENT, DATASET, MAX_LOAD_ATTEMPTS, TEMPLATE_ENV

schema = "NetSuite"

metadata = MetaData(schema=schema)

engine = create_engine(
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
        self._update()
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
        BQ_CLIENT.query(rendered_query).result()


class PostgresLoader(Loader):
    def __init__(self, model):
        self.model = Table(
            model.table,
            metadata,
            *[
                Column(
                    "_id",
                    Integer,
                    Identity(start=1, cycle=True, increment=1),
                    primary_key=True,
                ),
                *model.columns,
            ],
        )
        self.columns = model.columns

    def _copy(self, engine, rows):
        output_io = io.StringIO()
        csv_writer = csv.DictWriter(
            output_io,
            [c.name for c in self.model.c if not c.primary_key],
        )
        csv_writer.writeheader()
        csv_writer.writerows(rows)
        output_io.seek(0)
        column_names = ",".join(
            [f'"{c.name}"' for c in self.model.c if not c.primary_key]
        )
        copy_stmt = f'COPY "{schema}"."{self.model.name}" ({column_names}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
        raw_conn = engine.raw_connection()
        with raw_conn.cursor() as cur:
            cur.copy_expert(copy_stmt, output_io)
            raw_conn.commit()
        return cur


    def load(self, rows):
        self.model.create(bind=engine, checkfirst=True)
        with engine.connect().execution_options(autocommit=True) as conn:
            loads = self._load(engine, conn, rows)
        return {
            "load": "Postgres",
            "output_rows": loads,
        }

    @abstractmethod
    def _load(self, engine, conn, rows):
        pass


class PostgresStandardLoader(PostgresLoader):
    def _load(self, engine, conn, rows):
        truncate_stmt = f'TRUNCATE TABLE "{self.model.schema}"."{self.model.name}"'
        conn.execute(truncate_stmt)
        loads = self._copy(engine, rows)
        return loads.rowcount

class PostgresIncrementalLoader(PostgresLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys
        self.materialized_view = getattr(model, "materialized_view", None)

    def _dump_io(self, rows):
        output = io.StringIO()
        csv_writer = csv.DictWriter(
            output,
            [c.name for c in self.model.c if not c.primary_key],
        )
        csv_writer.writeheader()
        csv_writer.writerows(rows)
        output.seek(0)
        return output

    def _load(self, engine, conn, rows):
        loads = self._copy(engine, rows)
        sbq = select(
            [
                self.model.c._id,
                func.row_number()
                .over(
                    partition_by=[
                        self.model.c[c.name]
                        for c in self.model.c
                        if c.name in self.keys["p_key"]
                    ],
                    order_by=[
                        getattr(self.model.c, i).desc()
                        for i in self.keys["row_num_incre_key"]
                    ],
                )
                .label("row_num"),
                func.rank()
                .over(
                    partition_by=[
                        self.model.c[c.name]
                        for c in self.model.c
                        if c.name in self.keys["p_key"]!!!!!
                    ],
                    order_by=[
                        getattr(self.model.c, i).desc()
                        for i in self.keys["rank_incre_key"]
                    ],
                )
                .label("rank_num"),
            ]
        ).subquery("num")
        cte = (
            select(sbq.c._id)
            .where(sbq.c.row_num == 1)
            .where(sbq.c.rank_num == 1)
            .cte("cte")
        )
        delete_stmt = delete(self.model).where(~self.model.c._id.in_(select(cte.c._id)))
        x = str(delete_stmt)
        print(x)
        delll = conn.execute(delete_stmt)
        delll

        if self.materialized_view:
            conn.execute(
            f'REFRESH MATERIALIZED VIEW CONCURRENTLY "NetSuite"."{self.materialized_view}"'
        )

        return loads.rowcount
