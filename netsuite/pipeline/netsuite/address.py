from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ADDRESS",
    [
        {"name": "ADDRESS", "type": "STRING"},
        {"name": "ADDRESS_ID", "type": "INTEGER"},
        {"name": "ADDRESS_LINE_1", "type": "STRING"},
        {"name": "ADDRESS_LINE_2", "type": "STRING"},
        {"name": "ADDRESS_LINE_3", "type": "STRING"},
        {"name": "ATTENTION", "type": "STRING"},
        {"name": "CITY", "type": "STRING"},
        {"name": "CITYPROVINCE_ID", "type": "INTEGER"},
        {"name": "COMPANY", "type": "STRING"},
        {"name": "COUNTRY", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DISTRICTS_ID", "type": "INTEGER"},
        {"name": "IS_OVERRIDDEN", "type": "STRING"},
        {"name": "PHONE", "type": "STRING"},
        {"name": "STATE", "type": "STRING"},
        {"name": "ZIP", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            ADDRESS,
            ADDRESS_ID,
            ADDRESS_LINE_1,
            ADDRESS_LINE_2,
            ADDRESS_LINE_3,
            ATTENTION,
            CITY,
            CITYPROVINCE_ID,
            COMPANY,
            COUNTRY,
            DATE_LAST_MODIFIED,
            DISTRICTS_ID,
            IS_OVERRIDDEN,
            PHONE,
            STATE,
            ZIP
        FROM
            "Vua Nem Joint Stock Company".Administrator.ADDRESSES
        WHERE
            DATE_LAST_MODIFIED >= '{tr[0]}'
            AND DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["ADDRESS_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
