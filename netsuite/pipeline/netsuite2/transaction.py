from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ns2_transaction",
    [
        {"name": "id", "type": "INTEGER"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
        {"name": "recordtype", "type": "STRING"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT
            id,
            lastmodifieddate,
            recordtype
        FROM
            transaction
        WHERE
            lastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND lastmodifieddate <= TO_DATE('{tr[1]}', 'YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["id"],
        cursor_key=["lastmodifieddate"],
    ),
    load_callback_fn=update,
)
