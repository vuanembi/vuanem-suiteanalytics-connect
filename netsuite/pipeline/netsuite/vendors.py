from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "VENDORS",
    [
        {"name": "VENDOR_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "VENDOR_TYPE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            VENDORS.VENDOR_ID,
            VENDORS.NAME,
            VENDORS.FULL_NAME,
            VENDOR_TYPES.NAME AS 'VENDOR_TYPE'
        FROM
            "Vua Nem Joint Stock Company".Administrator.VENDORS
        LEFT JOIN "Vua Nem Joint Stock Company".Administrator.VENDOR_TYPES
        ON VENDORS.VENDOR_TYPE_ID = VENDOR_TYPES.VENDOR_TYPE_ID
    """,
)
