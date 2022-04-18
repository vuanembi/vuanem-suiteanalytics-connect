from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "SYSTEM_NOTES_PRICE",
    [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "VALUE_NEW", "type": "STRING"},
        {"name": "OPERATION", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            DATE_CREATED,
            ITEM_ID,
            VALUE_NEW,
            OPERATION
        FROM
            "Vua Nem Joint Stock Company".Administrator.SYSTEM_NOTES
        WHERE
            SYSTEM_NOTES.STANDARD_FIELD = 'RATE'
            AND SYSTEM_NOTES.ITEM_ID IS NOT NULL
    """,
)
