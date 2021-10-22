from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class DELIVERY_PERSON(NetSuite):
    query = """
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
    """
    schema = [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_PERSON_EXTID", "type": "STRING"},
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON_NAME", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "REF__EMPLOYEE_ID", "type": "INTEGER"},
        {"name": "VN_CODE", "type": "STRING"},
    ]
    columns = [
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("DELIVERY_PERSON_EXTID", String),
        Column("DELIVERY_PERSON_ID", Integer, primary_key=True),
        Column("DELIVERY_PERSON_NAME", String),
        Column("IS_INACTIVE", String),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("REF__EMPLOYEE_ID", Integer),
        Column("VN_CODE", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
