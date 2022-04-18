from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "DEPARTMENTS",
    [
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DEPARTMENT_DESCRIPTION", "type": "STRING"},
        {"name": "DEPARTMENT_EXTID", "type": "STRING"},
        {"name": "DEPARTMENT_ID", "type": "INTEGER"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "IS_INCLUDING_CHILD_SUBS", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "PARENT_ID", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            DATE_LAST_MODIFIED,
            DEPARTMENT_DESCRIPTION,
            DEPARTMENT_EXTID,
            DEPARTMENT_ID,
            FULL_NAME,
            ISINACTIVE,
            IS_INCLUDING_CHILD_SUBS,
            NAME,
            PARENT_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.DEPARTMENTS
    """,
)
