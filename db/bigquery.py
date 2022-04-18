from typing import Any, Optional, Callable
from datetime import datetime

from google.cloud import bigquery

from netsuite.pipeline.interface import Key

client = bigquery.Client()

DATASET = "DEV_NetSuite"


def _get_latest(start_fn: Callable[[Any], str], end_fn: Callable[[], Any]):
    def _get(table: str, key: Key):
        def __get(range_: tuple[Optional[str], Optional[str]]) -> tuple[str, str]:
            start, end = range_
            if start and end:
                return start, end
            else:
                maxs = [f"MAX({_key})" for _key in key.cursor_key]
                rows = client.query(
                    f"""
                    SELECT LEAST({','.join(maxs)}) AS cursor
                    FROM {DATASET}.{table}
                    """
                ).result()
                return start_fn([row for row in rows][0]["cursor"]), end_fn()

        return __get

    return _get


timeframe_builder = _get_latest(
    lambda x: x.date().isoformat(),
    lambda: datetime.utcnow().date().isoformat(),
)
id_builder = _get_latest(
    lambda x: x,
    lambda: int(50e7),
)


def load(table: str, schema: list[dict[str, Any]], key: Optional[Key]):
    def _load(data: list[dict[str, Any]]) -> int:
        if len(data) == 0:
            return 0

        output_rows = (
            client.load_table_from_json(
                data,
                f"{DATASET}.{table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND" if key else "WRITE_TRUNCATE",
                    schema=schema,
                ),
            )
            .result()
            .output_rows
        )
        return output_rows

    return _load


def update(table: str, key: Key):
    def _update(output_rows: int):
        if output_rows == 0:
            return output_rows
            
        id_key = ",".join(key.id_key)
        cursor_rn_key = ",".join(key.cursor_rn_key)
        rank_key = ",".join(key.rank_key)
        cursor_rank_key = ",".join(key.cursor_rank_key)
        query = f"""
        CREATE OR REPLACE TABLE {DATASET}.{table} AS
        SELECT * EXCEPT (row_num, _rank)
        FROM
        (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY {id_key}
                    ORDER BY {cursor_rn_key} DESC
                ) AS row_num,
            RANK() OVER (
                    PARTITION BY {rank_key}
                    ORDER BY {cursor_rank_key} DESC
                ) AS _rank,
            FROM
                {DATASET}.{table}
        )
        WHERE row_num = 1 AND _rank = 1
        """
        client.query(query).result()
        return output_rows

    return _update
