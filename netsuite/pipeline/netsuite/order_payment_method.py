from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "ORDER_PAYMENT_METHOD",
    [
        {"name": "ORDER_PAYMENT_METHOD_ID", "type": "INTEGER"},
        {"name": "ORDER_PAYMENT_METHOD_NAME", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            ORDER_PAYMENT_METHOD_ID,
            ORDER_PAYMENT_METHOD_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.ORDER_PAYMENT_METHOD
    """,
)
