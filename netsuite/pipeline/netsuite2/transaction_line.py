from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ns2_transactionLine",
    [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "netamount", "type": "FLOAT"},
        {"name": "rate", "type": "FLOAT"},
        {"name": "rateamount", "type": "FLOAT"},
        {"name": "linelastmodifieddate", "type": "TIMESTAMP"},
        {"name": "ratepercent", "type": "FLOAT"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT
            transaction AS TRANSACTION_ID,
            expenseaccount AS ACCOUNT_ID,
            netamount,
            rate,
            rateamount,
            linelastmodifieddate,
            ratepercent
        FROM
            transactionLine
        WHERE
            linelastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND linelastmodifieddate <= TO_DATE('{tr[1]}', 'YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["TRANSACTION_ID"],
        cursor_key=["linelastmodifieddate"],
    ),
    load_callback_fn=update,
)
