from sqlalchemy import Column, Integer, String, DateTime, BigInteger

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class TRANSACTIONS_DUE_DATE(NetSuite):
    query = """
        SELECT
            TRANSACTION_ID,
            TRANID,
            t.due_date,
            e.EMPLOYEE_ID,
            e.NAME AS PIC,
            it.DISPLAYNAME,
            t.STATUS,
            tl.ITEM_COUNT,
            tl.AMOUNT
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS t
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES tl ON t.TRANSACTION_ID = tl.TRANSACTION_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.ITEMS it ON tl.ITEM_ID = it.ITEM_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.EMPLOYEES e ON t.SALES_REP_ID = e.EMPLOYEE_ID
        WHERE
            t.DUE_DATE is NOT NULL
            AND t.TRANSACTION_TYPE = 'Purchase Order'
            AND tl.ITEM_ID IS NOT NULL
            AND it.DISPLAYNAME IS NOT NULL
    """
    schema = [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANID", "type": "STRING"},
        {"name": "due_date", "type": "TIMESTAMP"},
        {"name": "EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "PIC", "type": "STRING"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "AMOUNT", "type": "INTEGER"},
    ]
    columns = [
        Column("TRANSACTION_ID", Integer),
        Column("TRANID", String),
        Column("due_date", DateTime(timezone=True)),
        Column("EMPLOYEE_ID", Integer),
        Column("PIC", String),
        Column("DISPLAYNAME", String),
        Column("STATUS", String),
        Column("ITEM_COUNT", Integer),
        Column("AMOUNT", BigInteger),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
