from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "SUPPORT_PERSON_MAP",
    [
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            SPM.DELIVERY_PERSON_ID,
            SPM.TRANSACTION_ID,
            T.DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUPPORT_PERSON_MAP SPM
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS T
            ON SPM.TRANSACTION_ID = T.TRANSACTION_ID
        WHERE
            T.DATE_LAST_MODIFIED >= '{tr[0]}'
            AND T.DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["DELIVERY_PERSON_ID", "TRANSACTION_ID"],
        rank_key=["TRANSACTION_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
        cursor_rank_key=["DATE_LAST_MODIFIED"],
        cursor_rn_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
