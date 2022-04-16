from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "ACCOUNTS",
    [
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "FULL_DESCRIPTION", "type": "STRING"},
        {"name": "DESCRIPTION", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "HEADER_0", "type": "STRING"},
        {"name": "SUBHEADER", "type": "STRING"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "ACCOUNTNUMBER", "type": "STRING"},
        {"name": "TYPE_NAME", "type": "STRING"},
    ],
    netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            ACCOUNT_ID,
            DATE_LAST_MODIFIED,
            FULL_DESCRIPTION,
            DESCRIPTION,
            FULL_NAME,
            HEADER_0,
            SUBHEADER,
            LOCATION_ID,
            NAME,
            ACCOUNTNUMBER,
            TYPE_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.ACCOUNTS
    """,
)
