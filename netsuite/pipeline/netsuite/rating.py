from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "RATING",
    [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_RECORD_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LIST_ID", "type": "INTEGER"},
        {"name": "LIST_ITEM_NAME", "type": "STRING"},
        {"name": "RATING_EXTID", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            DATE_CREATED,
            IS_RECORD_INACTIVE,
            LAST_MODIFIED_DATE,
            LIST_ID,
            LIST_ITEM_NAME,
            RATING_EXTID
        FROM
            "Vua Nem Joint Stock Company".Administrator.RATING
        WHERE
            RATING.LAST_MODIFIED_DATE >= '{tr[0]}'
            AND RATING.LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["LIST_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
