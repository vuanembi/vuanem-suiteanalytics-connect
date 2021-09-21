from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class DEPARTMENTS(NetSuite):
    query = """
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
    """
    schema = [
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DEPARTMENT_DESCRIPTION", "type": "STRING"},
        {"name": "DEPARTMENT_EXTID", "type": "STRING"},
        {"name": "DEPARTMENT_ID", "type": "INTEGER"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "IS_INCLUDING_CHILD_SUBS", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "PARENT_ID", "type": "INTEGER"},
    ]
    columns = [
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("DEPARTMENT_DESCRIPTION", String),
        Column("DEPARTMENT_EXTID", String),
        Column("DEPARTMENT_ID", Integer, primary_key=True),
        Column("FULL_NAME", String),
        Column("ISINACTIVE", String),
        Column("IS_INCLUDING_CHILD_SUBS", String),
        Column("NAME", String),
        Column("PARENT_ID", Integer),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]
