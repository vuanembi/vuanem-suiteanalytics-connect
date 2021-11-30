from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class CLASSES(NetSuite):
    query = """
        SELECT
            CLASS_ID,
            DATE_LAST_MODIFIED,
            FULL_NAME,
            ISINACTIVE,
            CLASS_DESCRIPTION,
            NAME,
            PRODUCT_GROUP_CODE
        FROM
            "Vua Nem Joint Stock Company".Administrator.CLASSES
    """
    schema = [
        {"name": "CLASS_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "CLASS_DESCRIPTION", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "PRODUCT_GROUP_CODE", "type": "STRING"},
    ]
    columns = [
        Column("CLASS_ID", Integer),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("FULL_NAME", String),
        Column("ISINACTIVE", String),
        Column("CLASS_DESCRIPTION", String),
        Column("NAME", String),
        Column("PRODUCT_GROUP_CODE", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
