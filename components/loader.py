import time
from datetime import datetime
from abc import ABCMeta, abstractmethod

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from .utils import BQ_CLIENT, DATASET, MAX_LOAD_ATTEMPTS, TEMPLATE_ENV, ENGINE


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