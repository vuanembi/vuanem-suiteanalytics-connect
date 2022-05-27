from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update


pipeline = Pipeline(
    "ns2_transactionAddressMapping",
    [
        {"name": "address", "type": "INTEGER"},
        {"name": "addresstype", "type": "STRING"},
        {"name": "createdby", "type": "INTEGER"},
        {"name": "createddate", "type": "TIMESTAMP"},
        {"name": "id", "type": "INTEGER"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
        {"name": "transaction", "type": "INTEGER"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT
            address,
            addresstype,
            createdby,
            createddate,
            id,
            lastmodifieddate,
            transaction
        FROM
            transactionAddressMapping
        WHERE
            lastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND lastmodifieddate <= TO_DATE('{tr[1]}', 'YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["id"],
        cursor_key=["lastmodifieddate"],
    ),
    load_callback_fn=update,
)
