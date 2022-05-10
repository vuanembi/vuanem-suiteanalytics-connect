from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ACCOUNTING_PERIODS",
    [
        {"name": "ACCOUNTING_PERIOD_ID", "type": "INTEGER"},
        {"name": "CLOSED", "type": "STRING"},
        {"name": "CLOSED_ACCOUNTS_PAYABLE", "type": "STRING"},
        {"name": "CLOSED_ACCOUNTS_RECEIVABLE", "type": "STRING"},
        {"name": "CLOSED_ALL", "type": "STRING"},
        {"name": "CLOSED_ON", "type": "TIMESTAMP"},
        {"name": "CLOSED_PAYROLL", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "ENDING", "type": "TIMESTAMP"},
        {"name": "FISCAL_CALENDAR_ID", "type": "INTEGER"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "IS_ADJUSTMENT", "type": "STRING"},
        {"name": "LOCKED_ACCOUNTS_PAYABLE", "type": "STRING"},
        {"name": "LOCKED_ACCOUNTS_RECEIVABLE", "type": "STRING"},
        {"name": "LOCKED_ALL", "type": "STRING"},
        {"name": "LOCKED_PAYROLL", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "QUARTER", "type": "STRING"},
        {"name": "STARTING", "type": "TIMESTAMP"},
        {"name": "YEAR_0", "type": "STRING"},
        {"name": "YEAR_ID", "type": "INTEGER"},
    ],
    netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            ACCOUNTING_PERIOD_ID,
            CLOSED,
            CLOSED_ACCOUNTS_PAYABLE,
            CLOSED_ACCOUNTS_RECEIVABLE,
            CLOSED_ALL,
            CLOSED_ON,
            CLOSED_PAYROLL,
            DATE_LAST_MODIFIED,
            ENDING,
            FISCAL_CALENDAR_ID,
            FULL_NAME,
            ISINACTIVE,
            IS_ADJUSTMENT,
            LOCKED_ACCOUNTS_PAYABLE,
            LOCKED_ACCOUNTS_RECEIVABLE,
            LOCKED_ALL,
            LOCKED_PAYROLL,
            NAME,
            PARENT_ID,
            QUARTER,
            STARTING,
            YEAR_0,
            YEAR_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.ACCOUNTING_PERIODS
        WHERE
            DATE_LAST_MODIFIED >= '{tr[0]}'
            AND DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["ACCOUNTING_PERIOD_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
