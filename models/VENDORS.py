from sqlalchemy import Column, Integer, String

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class VENDORS(NetSuite):
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            VENDORS.VENDOR_ID,
            VENDORS.NAME,
            VENDORS.FULL_NAME,
            VENDOR_TYPES.NAME AS 'VENDOR_TYPE'
        FROM
            "Vua Nem Joint Stock Company".Administrator.VENDORS
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.VENDOR_TYPES ON VENDORS.VENDOR_TYPE_ID = VENDOR_TYPES.VENDOR_TYPE_ID
    """
    schema = [
        {"name": "VENDOR_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "VENDOR_TYPE", "type": "STRING"},
    ]
    columns = [
        Column("VENDOR_ID", Integer),
        Column("NAME", String),
        Column("FULL_NAME", String),
        Column("VENDOR_TYPE", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
