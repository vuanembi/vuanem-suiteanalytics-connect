from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class LOYALTY_CUSTOMER_GROUP(NetSuite):
    keys = {
        "p_key": ["LOYALTY_CUSTOMER_GROUP_ID"],
        "rank_key": ["LOYALTY_CUSTOMER_GROUP_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            LOYALTY_CUSTOMER_GROUP_ID,
            LAST_MODIFIED_DATE,
            DATE_CREATED,
            IS_INACTIVE,
            LEVEL_0,
            LOYALTY_CUSTOMER_GROUP_EXTID,
            LOYALTY_CUSTOMER_GROUP_NAME,
            MIN_REDEEMABLE_POINT,
            PARENT_ID,
            POINT_LEVEL_FROM,
            POINT_LEVEL_TO
        FROM
            "Vua Nem Joint Stock Company".Administrator.LOYALTY_CUSTOMER_GROUP
        WHERE
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
        {"name": "LOYALTY_CUSTOMER_GROUP_ID", "type": "INTEGER"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LEVEL_0", "type": "INTEGER"},
        {"name": "LOYALTY_CUSTOMER_GROUP_EXTID", "type": "STRING"},
        {"name": "LOYALTY_CUSTOMER_GROUP_NAME", "type": "STRING"},
        {"name": "MIN_REDEEMABLE_POINT", "type": "INTEGER"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "POINT_LEVEL_FROM", "type": "INTEGER"},
        {"name": "POINT_LEVEL_TO", "type": "INTEGER"},
    ]
    columns = [
        Column("LOYALTY_CUSTOMER_GROUP_ID", Integer, primary_key=True),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("IS_INACTIVE", String),
        Column("LEVEL_0", Integer),
        Column("LOYALTY_CUSTOMER_GROUP_EXTID", String),
        Column("LOYALTY_CUSTOMER_GROUP_NAME", String),
        Column("MIN_REDEEMABLE_POINT", Integer),
        Column("PARENT_ID", Integer),
        Column("POINT_LEVEL_FROM", Integer),
        Column("POINT_LEVEL_TO", Integer),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]
