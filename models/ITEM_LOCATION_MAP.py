from sqlalchemy import Column, Integer, String

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ITEM_LOCATION_MAP(NetSuite):
    query = """
        SELECT
            NEW_ITEM_CODE,
            ITEM_ID,
            LOCATION_ID,
            i.DISPLAYNAME,
            ON_HAND_COUNT
        FROM
            "Vua Nem Joint Stock Company".Administrator.ITEM_LOCATION_MAP ilm
        LEFT JOIN
            "Vua Nem Joint Stock Company".Administrator.ITEMS i
        ON
            ilm.NEW_ITEM_CODE = i.NEW_ITEM_CODE
    """
    schema = [
        {"name": "NEW_ITEM_CODE", "type": "STRING"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "ON_HAND_COUNT", "type": "INTEGER"},
    ]
    columns = [
        Column("NEW_ITEM_CODE", String),
        Column("ITEM_ID", Integer),
        Column("LOCATION_ID", Integer),
        Column("DISPLAYNAME", String),
        Column("ON_HAND_COUNT", Integer),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
