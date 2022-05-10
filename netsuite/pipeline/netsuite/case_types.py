from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "CASE_TYPES",
    [
        {"name": "CASE_TYPE_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
    ],
    netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            CASE_TYPE_ID,
            NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.CASE_TYPES
    """,
)
