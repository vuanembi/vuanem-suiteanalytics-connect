from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ACCOUNTS(NetSuite):
    query = """
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
    """
    schema = [
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
    ]
    columns = [
        Column("ACCOUNT_ID", Integer, primary_key=True),
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
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]
