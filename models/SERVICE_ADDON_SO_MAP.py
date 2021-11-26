from sqlalchemy import Column, Integer, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class SERVICE_ADDON_SO_MAP(NetSuite):
    keys = {
        "p_key": ["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_SO_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            SERVICE_ADDON_SO_MAP.LIST_SERVICE_ADD_ON_SO_ID,
            SERVICE_ADDON_SO_MAP.TRANSACTION_ID,
            TRANSACTIONS.DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SERVICE_ADDON_SO_MAP
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS ON SERVICE_ADDON_SO_MAP.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
        WHERE
            TRANSACTIONS.DATE_LAST_MODIFIED >= '{{ start }}'
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
        {"name": "LIST_SERVICE_ADD_ON_SO_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("LIST_SERVICE_ADD_ON_SO_ID", Integer),
        Column("TRANSACTION_ID", Integer),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
