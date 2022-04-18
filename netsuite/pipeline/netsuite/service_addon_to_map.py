from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "SERVICE_ADDON_SO_MAP",
    [
        {"name": "LIST_SERVICE_ADD_ON_TO_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            LIST_SERVICE_ADD_ON_TO_ID,
            TRANSACTION_ID,
            TRANSACTIONS.DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SERVICE_ADDON_TO_MAP
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS ON SERVICE_ADDON_TO_MAP.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
        WHERE
            TRANSACTIONS.DATE_LAST_MODIFIED >= '{tr[0]}'
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_TO_ID"],
        rank_key=["TRANSACTION_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
        cursor_rank_key=["DATE_LAST_MODIFIED"],
        cursor_rn_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
