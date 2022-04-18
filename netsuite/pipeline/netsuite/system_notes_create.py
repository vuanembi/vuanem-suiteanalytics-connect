from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "SYSTEM_NOTES_CREATE",
    [
        {"name": "AUTHOR_ID", "type": "INTEGER"},
        {"name": "COMPANY_ID", "type": "INTEGER"},
        {"name": "CONTEXT_TYPE_NAME", "type": "STRING"},
        {"name": "CUSTOM_FIELD", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "EVENT_ID", "type": "INTEGER"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "LINE_ID", "type": "INTEGER"},
        {"name": "LINE_TRANSACTION_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NOTE_TYPE_ID", "type": "INTEGER"},
        {"name": "OPERATION", "type": "STRING"},
        {"name": "RECORD_ID", "type": "INTEGER"},
        {"name": "RECORD_TYPE_ID", "type": "INTEGER"},
        {"name": "ROLE_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "VALUE_NEW", "type": "STRING"},
        {"name": "VALUE_OLD", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            AUTHOR_ID,
            COMPANY_ID,
            CONTEXT_TYPE_NAME,
            CUSTOM_FIELD,
            DATE_CREATED,
            EVENT_ID,
            ITEM_ID,
            LINE_ID,
            LINE_TRANSACTION_ID,
            NAME,
            NOTE_TYPE_ID,
            OPERATION,
            RECORD_ID,
            RECORD_TYPE_ID,
            ROLE_ID,
            TRANSACTION_ID,
            VALUE_NEW,
            VALUE_OLD
        FROM
            "Vua Nem Joint Stock Company"."Administrator".SYSTEM_NOTES
        WHERE
            (
                DATE_CREATED >= '{tr[0]}'
                AND DATE_CREATED <= '{tr[1]}'
            )
            AND OPERATION LIKE '%Create%'
            AND COMPANY_ID IS NOT NULL
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=[
            "RECORD_ID",
            "RECORD_TYPE_ID",
            "LINE_TRANSACTION_ID",
            "EVENT_ID",
            "ITEM_ID",
            "AUTHOR_ID",
            "COMPANY_ID",
            "NAME",
        ],
        cursor_key=["DATE_CREATED"],
    ),
    load_callback_fn=update,
)
