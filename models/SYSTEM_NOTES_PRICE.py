from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class SYSTEM_NOTES_PRICE(NetSuite):
    query = """
        SELECT
            DATE_CREATED,
            ITEM_ID,
            VALUE_NEW,
            OPERATION
        FROM
            "Vua Nem Joint Stock Company".Administrator.SYSTEM_NOTES
        WHERE
            SYSTEM_NOTES.STANDARD_FIELD = 'RATE'
            AND SYSTEM_NOTES.ITEM_ID IS NOT NULL
    """
    schema = [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "VALUE_NEW", "type": "STRING"},
        {"name": "OPERATION", "type": "STRING"},
    ]
    columns = [
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("ITEM_ID", Integer),
        Column("VALUE_NEW", String),
        Column("OPERATION", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
