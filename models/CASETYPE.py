from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class CASETYPE(NetSuite):
    query = """
        SELECT
            CASE_TYPE,
            CASE_TYPE_EXTID,
            DATE_LAST_MODIFIED,
            DESCRIPTION,
            IS_INACTIVE,
            NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.CASETYPE
    """
    schema = [
        {"name": "CASE_TYPE", "type": "INTEGER"},
        {"name": "CASE_TYPE_EXTID", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DESCRIPTION", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
    ]
    columns = [
        Column("ACCOUNT_ID", Integer),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("FULL_DESCRIPTION", String),
        Column("DESCRIPTION", String),
        Column("FULL_NAME", String),
        Column("HEADER_0", String),
        Column("SUBHEADER", String),
        Column("LOCATION_ID", Integer),
        Column("NAME", String),
        Column("ACCOUNTNUMBER", String),
        Column("TYPE_NAME", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
