from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "DELIVERY_METHOD",
    [
        {"name": "LIST_ID", "type": "INTEGER"},
        {"name": "LIST_ITEM_NAME", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            LIST_ID,
            LIST_ITEM_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.DELIVERY_METHOD
    """,
)
