from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "CLASSES",
    [
        {"name": "CLASS_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "CLASS_DESCRIPTION", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "PRODUCT_GROUP_CODE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            CLASS_ID,
            DATE_LAST_MODIFIED,
            FULL_NAME,
            ISINACTIVE,
            CLASS_DESCRIPTION,
            NAME,
            PRODUCT_GROUP_CODE
        FROM
            "Vua Nem Joint Stock Company".Administrator.CLASSES
    """,
)
