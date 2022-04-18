from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "CASETYPE",
    [
        {"name": "CASE_TYPE", "type": "INTEGER"},
        {"name": "CASE_TYPE_EXTID", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DESCRIPTION", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
    ],
    netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            CASE_TYPE,
            CASE_TYPE_EXTID,
            DATE_LAST_MODIFIED,
            DESCRIPTION,
            IS_INACTIVE,
            NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.CASETYPE
    """,
)
