from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "TRANSACTION_LINES",
    [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_LINE_ID", "type": "INTEGER"},
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "AMOUNT_BEFORE_DISCOUNT", "type": "INTEGER"},
        {"name": "AMOUNT_FOREIGN_LINKED", "type": "INTEGER"},
        {"name": "CLASS_ID", "type": "INTEGER"},
        {"name": "COMPANY_ID", "type": "INTEGER"},
        {"name": "DATE_CLOSED", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_METHOD_ID", "type": "INTEGER"},
        {"name": "DELIVER_LOCATION_ID", "type": "INTEGER"},
        {"name": "DEPARTMENT_ID", "type": "INTEGER"},
        {"name": "EXPECTED_DELIVERY_DATE_SO", "type": "TIMESTAMP"},
        {"name": "GROSS_AMOUNT", "type": "INTEGER"},
        {"name": "IS_COST_LINE", "type": "STRING"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "ITEM_GROUP_PROMOTION_ID", "type": "INTEGER"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "ITEM_TYPE", "type": "STRING"},
        {"name": "ITEM_UNIT_PRICE", "type": "STRING"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "MEMO", "type": "STRING"},
        {"name": "NET_AMOUNT", "type": "INTEGER"},
        {"name": "QUANTITY_RECEIVED_IN_SHIPMENT", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ORDER", "type": "INTEGER"},
        {"name": "TRANSFER_ORDER_ITEM_LINE", "type": "INTEGER"},
        {"name": "TRANSFER_ORDER_LINE_TYPE", "type": "STRING"},
        {"name": "VENDOR_ID", "type": "INTEGER"},
        {"name": "TRANSACTIONS_DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda start, end: f"""
        SELECT
            TRANSACTION_LINES.TRANSACTION_ID,
            TRANSACTION_LINES.TRANSACTION_LINE_ID,
            TRANSACTION_LINES.ACCOUNT_ID,
            TRANSACTION_LINES.AMOUNT,
            TRANSACTION_LINES.AMOUNT_BEFORE_DISCOUNT,
            TRANSACTION_LINES.AMOUNT_FOREIGN_LINKED,
            TRANSACTION_LINES.CLASS_ID,
            TRANSACTION_LINES.COMPANY_ID,
            TRANSACTION_LINES.DATE_CLOSED,
            TRANSACTION_LINES.DATE_CREATED,
            TRANSACTION_LINES.DATE_LAST_MODIFIED_GMT AS DATE_LAST_MODIFIED,
            TRANSACTION_LINES.DELIVERY_METHOD_ID,
            TRANSACTION_LINES.DELIVER_LOCATION_ID,
            TRANSACTION_LINES.DEPARTMENT_ID,
            TRANSACTION_LINES.EXPECTED_DELIVERY_DATE_SO,
            TRANSACTION_LINES.GROSS_AMOUNT,
            TRANSACTION_LINES.IS_COST_LINE,
            TRANSACTION_LINES.ITEM_COUNT,
            TRANSACTION_LINES.ITEM_GROUP_PROMOTION_ID,
            TRANSACTION_LINES.ITEM_ID,
            TRANSACTION_LINES.ITEM_TYPE,
            TRANSACTION_LINES.ITEM_UNIT_PRICE,
            TRANSACTION_LINES.LOCATION_ID,
            TRANSACTION_LINES.MEMO,
            TRANSACTION_LINES.NET_AMOUNT,
            TRANSACTION_LINES.SUBSIDIARY_ID,
            TRANSACTION_LINES.QUANTITY_RECEIVED_IN_SHIPMENT,
            TRANSACTION_LINES.TRANSFER_ORDER_ITEM_LINE,
            TRANSACTION_LINES.TRANSFER_ORDER_LINE_TYPE,
            TRANSACTION_LINES.TRANSACTION_ORDER,
            TRANSACTION_LINES.VENDOR_ID,
            TRANSACTIONS.DATE_LAST_MODIFIED AS TRANSACTIONS_DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES AS TRANSACTION_LINES
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS AS TRANSACTIONS ON TRANSACTION_LINES.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
        WHERE
            (
                TRANSACTIONS.DATE_LAST_MODIFIED >= '{start}'
                OR TRANSACTION_LINES.DATE_LAST_MODIFIED_GMT >= '{start}'
            )
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{end}'
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
