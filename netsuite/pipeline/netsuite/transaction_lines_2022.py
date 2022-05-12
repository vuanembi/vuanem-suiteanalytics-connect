from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "TRANSACTION_LINES_2022",
    [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_LINE_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "TRANSACTIONS_DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "ESTIMATED_COST", "type": "INTEGER"},
        {"name": "PAYMENT_METHOD_ID", "type": "INTEGER"},
        {"name": "UNIQUE_KEY", "type": "INTEGER"},
        {"name": "NUMBER_BILLED", "type": "INTEGER"},
        {"name": "SHIPMENT_RECEIVED", "type": "TIMESTAMP"},
        {"name": "QUANTITY_COMMITTED", "type": "INTEGER"},
        {"name": "RATE_BEFORE_DISCOUNT", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            TRANSACTION_LINES.TRANSACTION_ID,
            TRANSACTION_LINES.TRANSACTION_LINE_ID,
            TRANSACTION_LINES.DATE_LAST_MODIFIED_GMT AS DATE_LAST_MODIFIED,
            TRANSACTIONS.DATE_LAST_MODIFIED AS TRANSACTIONS_DATE_LAST_MODIFIED,
            TRANSACTION_LINES.ESTIMATED_COST,
            TRANSACTION_LINES.PAYMENT_METHOD_ID,
            TRANSACTION_LINES.UNIQUE_KEY,
            TRANSACTION_LINES.NUMBER_BILLED,
            TRANSACTION_LINES.SHIPMENT_RECEIVED,
            TRANSACTION_LINES.QUANTITY_COMMITTED,
            TRANSACTION_LINES.RATE_BEFORE_DISCOUNT
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES AS TRANSACTION_LINES
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS AS TRANSACTIONS ON TRANSACTION_LINES.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
        WHERE
            (
                TRANSACTIONS.DATE_LAST_MODIFIED >= '{tr[0]}'
                OR TRANSACTION_LINES.DATE_LAST_MODIFIED_GMT >= '{tr[0]}'
            )
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{tr[1]}'
            AND TRANSACTIONS.DATE_LAST_MODIFIED >= '2022-01-01'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["TRANSACTION_ID", "TRANSACTION_LINE_ID"],
        rank_key=["TRANSACTION_ID"],
        cursor_key=["DATE_LAST_MODIFIED", "TRANSACTIONS_DATE_LAST_MODIFIED"],
        cursor_rank_key=["TRANSACTIONS_DATE_LAST_MODIFIED"],
        cursor_rn_key=["TRANSACTIONS_DATE_LAST_MODIFIED", "DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
