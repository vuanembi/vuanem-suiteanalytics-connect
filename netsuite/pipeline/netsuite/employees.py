from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "EMPLOYEES",
    [
        {"name": "EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "VN_CODE", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "FIRSTNAME", "type": "STRING"},
        {"name": "MIDDLENAME", "type": "STRING"},
        {"name": "LASTNAME", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "JOBTITLE", "type": "STRING"},
        {"name": "PHONE", "type": "STRING"},
        {"name": "MOBILE_PHONE", "type": "STRING"},
        {"name": "EMAIL", "type": "STRING"},
        {"name": "GENDER", "type": "STRING"},
        {"name": "BIRTHDATE", "type": "TIMESTAMP"},
        {"name": "COMMENTS", "type": "STRING"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "HIREDDATE", "type": "TIMESTAMP"},
        {"name": "ONBOARDING_DATE", "type": "TIMESTAMP"},
        {"name": "RELEASEDATE", "type": "TIMESTAMP"},
        {"name": "RELEASE_REASON_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON", "type": "STRING"},
        {"name": "DEPARTMENT_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "EMPLOYEE_TYPE_ID", "type": "INTEGER"},
        {"name": "IDENTITY_NO_", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_AGENCY", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_DATE", "type": "TIMESTAMP"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "ISSALESREP", "type": "STRING"},
        {"name": "JOB_DESCRIPTION", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "SUPERVISOR_ID", "type": "INTEGER"},
    ],
    netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            EMPLOYEE_ID,
            VN_CODE,
            FULL_NAME,
            FIRSTNAME,
            MIDDLENAME,
            LASTNAME,
            NAME,
            JOBTITLE,
            PHONE,
            MOBILE_PHONE,
            EMAIL,
            GENDER,
            BIRTHDATE,
            COMMENTS,
            CREATE_DATE,
            DATE_LAST_MODIFIED,
            LAST_MODIFIED_DATE,
            HIREDDATE,
            ONBOARDING_DATE,
            RELEASEDATE,
            RELEASE_REASON_ID,
            DELIVERY_PERSON,
            DEPARTMENT_ID,
            LOCATION_ID,
            EMPLOYEE_TYPE_ID,
            IDENTITY_NO_,
            IDENTITY_PROVIDED_AGENCY,
            IDENTITY_PROVIDED_DATE,
            ISINACTIVE,
            ISSALESREP,
            JOB_DESCRIPTION,
            STATUS,
            SUBSIDIARY_ID,
            SUPERVISOR_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.EMPLOYEES
    """,
)
