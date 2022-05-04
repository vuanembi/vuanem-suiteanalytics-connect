from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "IR",
    [
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
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
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
    """,
)
