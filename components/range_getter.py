from datetime import datetime

from jinja2 import Template


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


def get_latest_range(table, keys):
    query = f"""
    SELECT LEAST(MAX({','.join(keys['incre_key'])})) AS incre
    FROM DATASET.{table}
    """
    rows = BQ_CLIENT.query(query).result()
    row = [row for row in rows][0]
    return row["incre"]


class QueryBuilder:
    def __init__(self, query, start, end):
        self.model = model
        self.start, self.end = start, end


class StandardQueryBuilder(QueryBuilder):
    def build(self):
        return self.model.query

class IncrementalBuilder(QueryBuilder):
    def build(self):
        start, end = self.get_range()
        return Template()


class TimeQueryBuilder(QueryBuilder):
    def get_range(self):
        if self.start and self.end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [self.start, self.end]
            ]
        else:
            end = NOW.strftime(TIMESTAMP_FORMAT)
            start = get_latest_range().strftime(TIMESTAMP_FORMAT)
        return start, end


class IDQueryBuilder(QueryBuilder):
    def get_range(self):
        if self.start and self.end:
            start, end = self.start, self.end
        else:
            end = 50e7
            start = get_latest_range()
        return start, end
