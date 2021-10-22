from sqlalchemy import Column, Integer, String, DateTime, BigInteger

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class BUDGET(NetSuite):
    query = """
        SELECT
            BUDGET.BUDGET_ID,
            BUDGET.LOCATION_ID,
            ACCOUNTING_PERIODS.STARTING,
            ACCOUNTING_PERIODS.NAME AS PERIODS_NAME,
            BUDGET_CATEGORY.NAME AS CATEGORY_NAME,
            BUDGET.AMOUNT,
            BUDGET_CATEGORY.ISINACTIVE AS 'BUDGET_ISINACTIVE'
        FROM
            "Vua Nem Joint Stock Company".Administrator.BUDGET
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.ACCOUNTING_PERIODS
            ON BUDGET.ACCOUNTING_PERIOD_ID = ACCOUNTING_PERIODS.ACCOUNTING_PERIOD_ID
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.BUDGET_CATEGORY
            ON BUDGET.CATEGORY_ID = BUDGET_CATEGORY.BUDGET_CATEGORY_ID
    """
    schema = [
        {"name": "BUDGET_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STARTING", "type": "TIMESTAMP"},
        {"name": "PERIODS_NAME", "type": "STRING"},
        {"name": "CATEGORY_NAME", "type": "STRING"},
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "BUDGET_ISINACTIVE", "type": "STRING"},
    ]
    columns = [
        Column("BUDGET_ID", Integer, primary_key=True),
        Column("LOCATION_ID", Integer),
        Column("STARTING", DateTime(timezone=True)),
        Column("PERIODS_NAME", String),
        Column("CATEGORY_NAME", String),
        Column("AMOUNT", BigInteger),
        Column("BUDGET_ISINACTIVE", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
