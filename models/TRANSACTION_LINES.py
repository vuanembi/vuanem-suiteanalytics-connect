from sqlalchemy import Column, Integer, String, DateTime, BigInteger

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class TRANSACTION_LINES(NetSuite):
    keys = {
        "p_key": ["TRANSACTION_ID", "TRANSACTION_LINE_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED", "TRANSACTIONS_DATE_LAST_MODIFIED"],
        "rank_incre_key": ["TRANSACTIONS_DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["TRANSACTIONS_DATE_LAST_MODIFIED", "DATE_LAST_MODIFIED"],
    }
    query = """
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
                TRANSACTIONS.DATE_LAST_MODIFIED >= '{{ start }}'
                OR TRANSACTION_LINES.DATE_LAST_MODIFIED_GMT >= '{{ start }}'
            )
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
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
    ]
    columns = [
        Column("TRANSACTION_ID", Integer, primary_key=True),
        Column("TRANSACTION_LINE_ID", Integer, primary_key=True),
        Column("ACCOUNT_ID", Integer),
        Column("AMOUNT", BigInteger),
        Column("AMOUNT_BEFORE_DISCOUNT", BigInteger),
        Column("AMOUNT_FOREIGN_LINKED", BigInteger),
        Column("CLASS_ID", Integer),
        Column("COMPANY_ID", Integer),
        Column("DATE_CLOSED", DateTime(timezone=True)),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("DELIVERY_METHOD_ID", Integer),
        Column("DELIVER_LOCATION_ID", Integer),
        Column("DEPARTMENT_ID", Integer),
        Column("EXPECTED_DELIVERY_DATE_SO", DateTime(timezone=True)),
        Column("GROSS_AMOUNT", BigInteger),
        Column("IS_COST_LINE", String),
        Column("ITEM_COUNT", Integer),
        Column("ITEM_GROUP_PROMOTION_ID", Integer),
        Column("ITEM_ID", Integer),
        Column("ITEM_TYPE", String),
        Column("ITEM_UNIT_PRICE", String),
        Column("LOCATION_ID", Integer),
        Column("MEMO", String),
        Column("NET_AMOUNT", BigInteger),
        Column("QUANTITY_RECEIVED_IN_SHIPMENT", Integer),
        Column("SUBSIDIARY_ID", Integer),
        Column("TRANSACTION_ORDER", Integer),
        Column("TRANSFER_ORDER_ITEM_LINE", Integer),
        Column("TRANSFER_ORDER_LINE_TYPE", String),
        Column("VENDOR_ID", Integer),
        Column("TRANSACTIONS_DATE_LAST_MODIFIED", DateTime(timezone=True)),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]
