from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "DELETED_RECORDS",
    [
        {"name": "CUSTOM_RECORD_TYPE", "type": "STRING"},
        {"name": "DATE_DELETED", "type": "TIMESTAMP"},
        {"name": "ENTITY_ID", "type": "INTEGER"},
        {"name": "ENTITY_NAME", "type": "STRING"},
        {"name": "IS_CUSTOM_LIST", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "RECORD_BASE_TYPE", "type": "STRING"},
        {"name": "RECORD_ID", "type": "INTEGER"},
        {"name": "RECORD_TYPE_NAME", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            CUSTOM_RECORD_TYPE,
            DATE_DELETED,
            ENTITY_ID,
            ENTITY_NAME,
            IS_CUSTOM_LIST,
            NAME,
            RECORD_BASE_TYPE,
            RECORD_ID,
            RECORD_TYPE_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.DELETED_RECORDS
        WHERE
            DATE_DELETED >= '{tr[0]}'
            AND DATE_DELETED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["RECORD_ID"],
        cursor_key=["DATE_DELETED"],
    ),
    load_callback_fn=update,
)
