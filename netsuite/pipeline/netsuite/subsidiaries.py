from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "SUBSIDIARIES",
    [
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "STATE", "type": "STRING"},
        {"name": "SUBSIDIARY_EXTID", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            SUBSIDIARY_ID,
            STATE,
            SUBSIDIARY_EXTID
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUBSIDIARIES
    """,
)
