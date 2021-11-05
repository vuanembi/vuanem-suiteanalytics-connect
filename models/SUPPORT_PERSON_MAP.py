from sqlalchemy import Column, Integer, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class SUPPORT_PERSON_MAP(NetSuite):
    keys = {
        "p_key": ["DELIVERY_PERSON_ID", "TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            SPM.DELIVERY_PERSON_ID,
            SPM.TRANSACTION_ID,
            T.DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUPPORT_PERSON_MAP SPM
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS T ON SPM.TRANSACTION_ID = T.TRANSACTION_ID
        WHERE
            T.DATE_LAST_MODIFIED >= '{{ start }}'
            AND T.DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("DELIVERY_PERSON_ID", Integer),
        Column("TRANSACTION_ID", Integer),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
