from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ns2_transactionLine3",
    [
        {"name": "TRANSACTION_ID", "type": "NUMERIC"},
        {"name": "id", "type": "NUMERIC"},
        {"name": "quantity", "type": "NUMERIC"},
        {"name": "quantitybackordered", "type": "NUMERIC"},
        {"name": "quantitybilled", "type": "NUMERIC"},
        {"name": "quantitycommitted", "type": "NUMERIC"},
        {"name": "quantitypacked", "type": "NUMERIC"},
        {"name": "quantitypicked", "type": "NUMERIC"},
        {"name": "quantityrejected", "type": "NUMERIC"},
        {"name": "quantityshiprecv", "type": "NUMERIC"},
        {"name": "linelastmodifieddate", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT
            transaction AS TRANSACTION_ID,
            id,
            quantity,
            quantitybackordered,
            quantitybilled,
            quantitycommitted,
            quantitypacked,
            quantitypicked,
            quantityrejected,
            quantityshiprecv,
            linelastmodifieddate
        FROM
            transactionLine
        WHERE
            linelastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND linelastmodifieddate <= TO_DATE('{tr[1]}', 'YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["TRANSACTION_ID", "id"],
        cursor_key=["linelastmodifieddate"],
    ),
    load_callback_fn=update,
)
