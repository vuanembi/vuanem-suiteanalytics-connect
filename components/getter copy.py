from datetime import datetime
from abc import ABCMeta, abstractmethod

from jinja2 import Template
from google.api_core.exceptions import NotFound

from .utils import (
    BQ_CLIENT,
    DATASET,
    DATE_FORMAT,
    TIMESTAMP_FORMAT,
    NOW,
    TEMPLATE_ENV,
    QUERIES_ENV,
    ROWS_PER_FETCH,
)



class Getter(metaclass=ABCMeta):
    def __init__(self, model):
        self.connector = model._connector
        self.table = model.table
        self.template = Template(model.query)

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
