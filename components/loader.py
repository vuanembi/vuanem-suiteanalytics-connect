import time
from abc import ABCMeta, abstractmethod
import io
import csv
import json

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from sqlalchemy import MetaData, Table, delete, and_, select, func, desc
from sqlalchemy.dialects.postgresql import insert

from .utils import BQ_CLIENT, DATASET, MAX_LOAD_ATTEMPTS, TEMPLATE_ENV, get_engine

schema = "NetSuite"

metadata = MetaData(schema=schema)


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
        self.model = Table(model.table, metadata, *model.columns)
        self.columns = model.columns

    def load(self, rows):
        engine = get_engine()
        with engine.connect() as conn:
            loads = self._load(engine, conn, rows)
        engine.dispose()
        return {
            "load": "Postgres",
            "output_rows": loads,
        }

    @abstractmethod
    def _load(self, engine, conn, rows):
        pass


class PostgresStandardLoader(PostgresLoader):
    def _load(self, engine, conn, rows):
        self.model.create(bind=engine, checkfirst=True)
        truncate_stmt = f'TRUNCATE TABLE "{self.model.schema}"."{self.model.name}"'
        conn.execute(truncate_stmt)
        loads = conn.execute(insert(self.model), rows)
        return loads.inserted_primary_key_rows


class PostgresIncrementalLoader(PostgresLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys
        self.materialized_view = getattr(model, "materialized_view", None)

    def _create_temp(self, engine):
        temp_table = Table(
            f"temp_{self.model.name}",
            # MetaData(bind=engine),
            metadata,
            *[c.copy() for c in self.model.c],
            # prefixes=["TEMPORARY"],
        )
        temp_table.create(bind=engine, checkfirst=True)
        return temp_table

    def _dump_io(self, rows):
        output = io.StringIO()
        csv_writer = csv.DictWriter(
            output,
            [c.name for c in self.model.c],
        )
        csv_writer.writeheader()
        csv_writer.writerows(rows)
        output.seek(0)
        return output

    def _generate_copy_stmt(self):
        column_names = ",".join([f'"{c.name}"' for c in self.model.c])
        copy_stmt = f'COPY "{schema}"."temp_{self.model.name}" ({column_names}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
        return copy_stmt

    def _drop_temp_table(self, temp_table):
        temp_table.drop()

    def _delete_duplicates(self):
        

    def _load(self, engine, conn, rows):
        temp_table = self._create_temp(engine)
        try:
            output_io = self._dump_io(rows)
            copy_stmt = self._generate_copy_stmt()
            with engine.raw_connection() as raw_conn:
                with raw_conn.cursor() as cur:
                    cur.copy_expert(copy_stmt, output_io)
                    raw_conn.commit()
            insert_stmt = insert(self.model).from_select(
                temp_table.c, select(self.model)
            )
            conn.execute(insert_stmt)
            cte = select(
                *[
                    self.model.c[c.name]
                    for c in self.model.c
                    if c.name in self.keys["p_key"]
                ],
                func.row_number()
                .over(
                    partition_by=[
                        self.model.c[c.name]
                        for c in self.model.c
                        if c.name in self.keys["p_key"]
                    ],
                    order_by=desc(
                        getattr(self.model.c, self.keys["row_num_incre_key"])
                    ),
                )
                .label("row_num"),
                func.rank()
                .over(
                    partition_by=[
                        self.model.c[c.name]
                        for c in self.model.c
                        if c.name in self.keys["p_key"]
                    ],
                    order_by=desc(getattr(self.model.c, self.keys["rank_incre_key"])),
                )
                .label("rank"),
            )
        except:
            print(123)
        finally:
            temp_table.drop(engine)

        # conn2 = engine.connect().connection
        # raw_cur = conn2.cursor()
        # conn2.commit()
        # # raw_cur.copy_from(output, f'"NetSuite"."temp_{self.model.name}"',sep=',',null='', columns=[c.name for c in self.model.c])
        # raw_cur.copy_expert(copy_stmt, output)
        # conn2.commit()
        # raw_cur.execute('SELECT * FROM "NetSuite"."temp_CASES"')
        # x = raw_cur.fetchall()
        # x = output.getvalue()

        # stmt = insert(self.model).values(rows)
        # # update_dict = {c.name: c for c in stmt.excluded if not c.primary_key}
        # update_dict = {
        #     c.name: c for c in stmt.excluded if c.name not in self.keys["p_key"]
        # }
        # stmt = stmt.on_conflict_do_update(
        #     index_elements=self.model.primary_key.columns,
        #     set_=update_dict,
        # )
        # delete_stmt = delete(self.model).where(
        #     and_(
        #         *[
        #             self.model.c[p_key].in_([row[p_key] for row in rows])
        #             for p_key in self.keys["p_key"]
        #         ]
        #     )
        # )
        # loads = conn.execute(stmt)
        # loads = conn.execute(insert(self.model), rows)
        return 1

    def _refresh_materialized_view(self, conn):
        conn.execute(
            f"""
            REFRESH MATERIALIZED VIEW CONCURRENTLY "NetSuite".{self.materialized_view}
            """
        )
