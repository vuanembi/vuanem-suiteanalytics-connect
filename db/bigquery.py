from typing import Any, Optional
from datetime import datetime

from google.cloud import bigquery

from netsuite.pipeline.interface import Key

client = bigquery.Client()

DATASET = "IP_NetSuite"


def timeframe_builder(table: str, key: Key):
    def _get(timeframe: tuple[Optional[str], Optional[str]]) -> tuple[str, str]:
        start, end = timeframe
        if start and end:
            return start, end
        else:
            maxs = [f"MAX({_key})" for _key in key.cursor_key]
            rows = client.query(
                f"""SELECT LEAST({maxs.join(',')}) AS cursor
                FROM {DATASET}.{table}
                """
            ).result()
            return tuple(
                [
                    i.isoformat(timespec="seconds", sep=" ")
                    for i in [
                        [row for row in rows][0]["cursor"],
                        datetime.utcnow(),
                    ]
                ]
            )

    return _get


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
        id_key = key.id_key.join(",")
        cursor_rn_key = key.cursor_rn_key.join(",")
        rank_key = key.rank_key.join(",")
        cursor_rank_key = key.cursor_rank_key.join(",")
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
