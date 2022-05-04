from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "TRANSACTIONS_DUE_DATE",
    [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANID", "type": "STRING"},
        {"name": "due_date", "type": "TIMESTAMP"},
        {"name": "EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "PIC", "type": "STRING"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "AMOUNT", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
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
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTION_LINES tl
        ON t.TRANSACTION_ID = tl.TRANSACTION_ID
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.ITEMS it
        ON tl.ITEM_ID = it.ITEM_ID
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.EMPLOYEES e
        ON t.SALES_REP_ID = e.EMPLOYEE_ID
        WHERE
            t.DUE_DATE is NOT NULL
            AND t.TRANSACTION_TYPE = 'Purchase Order'
            AND tl.ITEM_ID IS NOT NULL
            AND it.DISPLAYNAME IS NOT NULL
    """,
)
