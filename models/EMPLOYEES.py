from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class EMPLOYEES(NetSuite):
    query = """
        SELECT
            EMPLOYEES.EMPLOYEE_ID,
            EMPLOYEES.VN_CODE,
            EMPLOYEES.FULL_NAME,
            EMPLOYEES.FIRSTNAME,
            EMPLOYEES.MIDDLENAME,
            EMPLOYEES.LASTNAME,
            EMPLOYEES.NAME,
            EMPLOYEES.JOBTITLE,
            EMPLOYEES.PHONE,
            EMPLOYEES.MOBILE_PHONE,
            EMPLOYEES.EMAIL,
            EMPLOYEES.GENDER,
            EMPLOYEES.BIRTHDATE,
            EMPLOYEES.COMMENTS,
            EMPLOYEES.CREATE_DATE,
            EMPLOYEES.DATE_LAST_MODIFIED,
            EMPLOYEES.LAST_MODIFIED_DATE,
            EMPLOYEES.HIREDDATE,
            EMPLOYEES.ONBOARDING_DATE,
            EMPLOYEES.RELEASEDATE,
            EMPLOYEES.RELEASE_REASON_ID,
            EMPLOYEES.DELIVERY_PERSON,
            EMPLOYEES.DEPARTMENT_ID,
            EMPLOYEES.LOCATION_ID,
            EMPLOYEES.EMPLOYEE_TYPE_ID,
            EMPLOYEES.IDENTITY_NO_,
            EMPLOYEES.IDENTITY_PROVIDED_AGENCY,
            EMPLOYEES.IDENTITY_PROVIDED_DATE,
            EMPLOYEES.ISINACTIVE,
            EMPLOYEES.ISSALESREP,
            EMPLOYEES.JOB_DESCRIPTION,
            EMPLOYEES.STATUS,
            EMPLOYEES.SUBSIDIARY_ID,
            EMPLOYEES.SUPERVISOR_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.EMPLOYEES
    """
    schema = [
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
    ]
    columns = [
        Column("EMPLOYEE_ID", Integer, primary_key=True),
        Column("VN_CODE", String),
        Column("FULL_NAME", String),
        Column("FIRSTNAME", String),
        Column("MIDDLENAME", String),
        Column("LASTNAME", String),
        Column("NAME", String),
        Column("JOBTITLE", String),
        Column("PHONE", String),
        Column("MOBILE_PHONE", String),
        Column("EMAIL", String),
        Column("GENDER", String),
        Column("BIRTHDATE", DateTime(timezone=True)),
        Column("COMMENTS", String),
        Column("CREATE_DATE", DateTime(timezone=True)),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("HIREDDATE", DateTime(timezone=True)),
        Column("ONBOARDING_DATE", DateTime(timezone=True)),
        Column("RELEASEDATE", DateTime(timezone=True)),
        Column("RELEASE_REASON_ID", Integer),
        Column("DELIVERY_PERSON", String),
        Column("DEPARTMENT_ID", Integer),
        Column("LOCATION_ID", Integer),
        Column("EMPLOYEE_TYPE_ID", Integer),
        Column("IDENTITY_NO_", String),
        Column("IDENTITY_PROVIDED_AGENCY", String),
        Column("IDENTITY_PROVIDED_DATE", DateTime(timezone=True)),
        Column("ISINACTIVE", String),
        Column("ISSALESREP", String),
        Column("JOB_DESCRIPTION", String),
        Column("STATUS", String),
        Column("SUBSIDIARY_ID", Integer),
        Column("SUPERVISOR_ID", Integer),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]
