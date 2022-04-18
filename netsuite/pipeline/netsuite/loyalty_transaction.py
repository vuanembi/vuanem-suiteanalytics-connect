from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "LOYALTY_TRANSACTION",
    [
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "CUSTOMER_ID", "type": "INTEGER"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DOCUMENT_NO", "type": "STRING"},
        {"name": "EXPIRED_DATE", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LOYALTY_CUSTOMER_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_LOCATION_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_PROGRAM_ID", "type": "INTEGER"},
        {"name": "LOYALTY_TRANSACTION_EXTID", "type": "STRING"},
        {"name": "LOYALTY_TRANSACTION_ID", "type": "INTEGER"},
        {"name": "POINT_0", "type": "INTEGER"},
        {"name": "REWARD_RATE", "type": "INTEGER"},
        {"name": "TRANSACTION_DATE", "type": "TIMESTAMP"},
        {"name": "TRANSACTION_TYPE", "type": "STRING"},
        {"name": "TRANS_ID", "type": "INTEGER"},
        {"name": "TRANS_LOCATION_ID", "type": "INTEGER"},
        {"name": "UPDATE_TIME_", "type": "TIMESTAMP"},
        {"name": "VALID_FROM_DATE", "type": "TIMESTAMP"},
        {"name": "VALID_TO_DATE", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            AMOUNT,
            CUSTOMER_ID,
            DATE_CREATED,
            DOCUMENT_NO,
            EXPIRED_DATE,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            LOYALTY_CUSTOMER_GROUP_ID,
            LOYALTY_LOCATION_GROUP_ID,
            LOYALTY_PROGRAM_ID,
            LOYALTY_TRANSACTION_EXTID,
            LOYALTY_TRANSACTION_ID,
            POINT_0,
            REWARD_RATE,
            TRANSACTION_DATE,
            TRANSACTION_TYPE,
            TRANS_ID,
            TRANS_LOCATION_ID,
            UPDATE_TIME_,
            VALID_FROM_DATE,
            VALID_TO_DATE
        FROM
            "Vua Nem Joint Stock Company".Administrator.LOYALTY_TRANSACTION
        WHERE
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["LOYALTY_TRANSACTION_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
