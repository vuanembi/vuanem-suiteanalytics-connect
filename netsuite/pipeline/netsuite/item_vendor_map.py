from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "ITEM_VENDOR_MAP",
    [
        {"name": "IS_PREFERRED", "type": "STRING"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "VENDOR_CODE", "type": "STRING"},
        {"name": "VENDOR_ID", "type": "INTEGER"},
    ],
    netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            IS_PREFERRED,
            ITEM_ID,
            SUBSIDIARY_ID,
            VENDOR_CODE,
            VENDOR_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.ITEM_VENDOR_MAP
    """,
)
