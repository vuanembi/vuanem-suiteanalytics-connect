from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "BUDGET",
    [
        {"name": "BUDGET_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STARTING", "type": "TIMESTAMP"},
        {"name": "PERIODS_NAME", "type": "STRING"},
        {"name": "CATEGORY_NAME", "type": "STRING"},
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "BUDGET_ISINACTIVE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
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
    """,
)
