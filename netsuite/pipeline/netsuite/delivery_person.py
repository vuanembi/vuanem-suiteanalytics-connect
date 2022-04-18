from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "DELIVERY_PERSON",
    [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_PERSON_EXTID", "type": "STRING"},
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON_NAME", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "REF__EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "VN_CODE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            DATE_CREATED,
            DELIVERY_PERSON_EXTID,
            DELIVERY_PERSON_ID,
            DELIVERY_PERSON_NAME,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            REF__EMPLOYEE_ID,
            VN_CODE
        FROM
            "Vua Nem Joint Stock Company".Administrator.DELIVERY_PERSON
    """,
)
