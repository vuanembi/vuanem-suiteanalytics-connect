from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "SUBSIDIARY_LOCATION_MAP",
    [
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            LOCATION_ID,
            SUBSIDIARY_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUBSIDIARY_LOCATION_MAP
    """,
)
