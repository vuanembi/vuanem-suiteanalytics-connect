from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class IR(NetSuite):
    query = """
        SELECT
            t.TRANID AS PO_TranID,
            t.DUE_DATE AS PO_DueDate,
            rct.TRANSACTION_ID AS IR_TransactionID,
            rct.TRANID AS IR_TranID,
            rct.TRANDATE,
            tl.NET_AMOUNT,
            tl.ITEM_COUNT,
            it.PRODUCT_CODE,
            it.DISPLAYNAME,
            e.EMPLOYEE_ID,
            e.NAME AS PIC
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS t
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS rct ON t.TRANSACTION_ID = rct.CREATED_FROM_ID
            INNER JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES tl ON rct.TRANSACTION_ID = tl.TRANSACTION_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.ITEMS it ON tl.ITEM_ID = it.ITEM_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.EMPLOYEES e ON rct.SALES_REP_ID = e.EMPLOYEE_ID
        WHERE
            t.DUE_DATE > '2021-06-01'
            AND t.TRANSACTION_TYPE = 'Purchase Order'
            AND tl.ITEM_ID IS NOT NULL
            AND it.DISPLAYNAME IS NOT NULL
    """
    schema = [
        {"name": "PO_TranID", "type": "STRING"},
        {"name": "PO_DueDate", "type": "TIMESTAMP"},
        {"name": "IR_TransactionID", "type": "INTEGER"},
        {"name": "IR_TranID", "type": "STRING"},
        {"name": "TRANDATE", "type": "TIMESTAMP"},
        {"name": "NET_AMOUNT", "type": "INTEGER"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "PRODUCT_CODE", "type": "STRING"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "PIC", "type": "STRING"},
    ]
    columns = [
        Column("PO_TranID", String),
        Column("PO_DueDate", DateTime(timezone=True)),
        Column("IR_TransactionID", Integer),
        Column("IR_TranID", String),
        Column("TRANDATE", DateTime(timezone=True)),
        Column("NET_AMOUNT", Integer),
        Column("ITEM_COUNT", Integer),
        Column("PRODUCT_CODE", String),
        Column("DISPLAYNAME", String),
        Column("EMPLOYEE_ID", Integer),
        Column("PIC", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
