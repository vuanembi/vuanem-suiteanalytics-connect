from pipelines import NetSuiteJob


class BUDGET(NetSuiteJob):
    query = """
    SELECT
        BUDGET.LOCATION_ID,
        ACCOUNTING_PERIODS.STARTING,
        ACCOUNTING_PERIODS.NAME AS PERIODS_NAME,
        BUDGET_CATEGORY.NAME AS CATEGORY_NAME,
        BUDGET.AMOUNT,
        BUDGET_CATEGORY.ISINACTIVE AS 'BUDGET_ISINACTIVE'
    FROM
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".BUDGET
        LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".ACCOUNTING_PERIODS
    ON BUDGET.ACCOUNTING_PERIOD_ID = ACCOUNTING_PERIODS.ACCOUNTING_PERIOD_ID
    LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".BUDGET_CATEGORY
    ON BUDGET.CATEGORY_ID = BUDGET_CATEGORY.BUDGET_CATEGORY_ID
    """

    schema = [
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STARTING", "type": "TIMESTAMP"},
        {"name": "PERIODS_NAME", "type": "STRING"},
        {"name": "CATEGORY_NAME", "type": "STRING"},
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "BUDGET_ISINACTIVE", "type": "STRING"},
    ]
