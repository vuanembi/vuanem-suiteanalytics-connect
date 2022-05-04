from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "QUANHUYEN_LIST",
    [
        {"name": "CITYPROVINE_ID", "type": "INTEGER"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "QUNHUYN_LIST_EXTID", "type": "STRING"},
        {"name": "QUNHUYN_LIST_ID", "type": "INTEGER"},
        {"name": "QUNHUYN_LIST_NAME", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            CITYPROVINE_ID,
            DATE_CREATED,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            PARENT_ID,
            QUNHUYN_LIST_EXTID,
            QUNHUYN_LIST_ID,
            QUNHUYN_LIST_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.QUẬNHUYỆN_LIST
        WHERE
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["QUNHUYN_LIST_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
