from typing import Any, Optional

from compose import compose

from db.bigquery import load
from netsuite.repo import get
from netsuite.pipeline.interface import Pipeline


def _transform_int(cols: list[str]):
    def _transform(key: str, value: Optional[Any]):
        return int(value) if key in cols and value else None

    return _transform


def transform_service(schema: Pipeline.schema):
    def _svc(data: list[dict[str, Any]]):
        transform_int = _transform_int(
            [i["name"] for i in schema if i["type"] == "INTEGER"]
        )
        return [{k: transform_int(k, v) for k, v in i.items()} for i in data]

    return _svc


def pipeline_service(pipeline: Pipeline):
    def _get(query: str):
        with pipeline.conn_fn() as conn:
            return get(conn, query)

    def _svc(body: dict):
        return compose(
            lambda x: {
                "table": pipeline.name,
                "output_rows": x,
            },
            pipeline.load_callback_fn(pipeline.table, pipeline.key),
            load(pipeline.name, pipeline.schema, pipeline.key),
            _get,
            pipeline.query_fn,
            pipeline.param_fn(pipeline.name, getattr(pipeline.key, "cursor_key")),
        )((body.get("start"), body.get("end")))

    return _svc
