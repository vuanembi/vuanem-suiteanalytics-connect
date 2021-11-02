import time
from abc import ABCMeta, abstractmethod

from google.cloud import bigquery
from google.api_core.exceptions import Forbidden
from sqlalchemy import delete, and_, insert

from .utils import BQ_CLIENT, DATASET, MAX_LOAD_ATTEMPTS, TEMPLATE_ENV, get_engine


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
        self.model = model.model

    def load(self, rows):
        engine = get_engine()
        with engine.connect() as conn:
            loads = self._load(engine, conn, rows)
        engine.dispose()
        return {
            "load": "Postgres",
            "output_rows": len(loads.inserted_primary_key_rows),
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
        return loads


class PostgresIncrementalLoader(PostgresLoader):
    def __init__(self, model):
        super().__init__(model)
        self.keys = model.keys
        self.materialized_view = getattr(model, 'materialized_view', None)

    def _load(self, engine, conn, rows):
        self.model.create(bind=engine, checkfirst=True)
        delete_stmt = delete(self.model).where(
            and_(
                *[
                    self.model.c[p_key].in_([row[p_key] for row in rows])
                    for p_key in self.keys["p_key"]
                ]
            )
        )
        conn.execute(delete_stmt)
        loads = conn.execute(insert(self.model), rows)
        return loads
    
    def _refresh_materialized_view(self, conn):
        conn.execute(f"""
            REFRESH MATERIALIZED VIEW CONCURRENTLY "NetSuite".{self.materialized_view}
            """
        )


