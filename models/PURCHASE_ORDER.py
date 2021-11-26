from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class PURCHASE_ORDER(NetSuite):
    query = """
        SELECT
            t.TRANID,
            t.TRANSACTION_ID,
            t.TRANDATE,
            t.DUE_DATE,
            tl.NET_AMOUNT,
            tl.ITEM_COUNT,
            it.PRODUCT_CODE,
            it.DISPLAYNAME,
            e.EMPLOYEE_ID,
            e.NAME AS PIC
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS t
            INNER JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES tl ON t.TRANSACTION_ID = tl.TRANSACTION_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.ITEMS it ON tl.ITEM_ID = it.ITEM_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.EMPLOYEES e ON t.SALES_REP_ID = e.EMPLOYEE_ID
        WHERE
            t.DUE_DATE >= '2021-06-01'
            AND t.TRANSACTION_TYPE = 'Purchase Order'
            AND tl.ITEM_ID IS NOT NULL
            AND it.DISPLAYNAME IS NOT NULL
    """
    schema = [
        {"name": "TRANID", "type": "STRING"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANDATE", "type": "TIMESTAMP"},
        {"name": "DUE_DATE", "type": "TIMESTAMP"},
        {"name": "NET_AMOUNT", "type": "INTEGER"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "PRODUCT_CODE", "type": "STRING"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "PIC", "type": "STRING"},
    ]
    columns = [
        Column("TRANID", String),
        Column("TRANSACTION_ID", Integer),
        Column("TRANDATE", DateTime(timezone=True)),
        Column("DUE_DATE", DateTime(timezone=True)),
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
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
