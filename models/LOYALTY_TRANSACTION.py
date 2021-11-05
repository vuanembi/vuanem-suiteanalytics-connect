from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class LOYALTY_TRANSACTION(NetSuite):
    keys = {
        "p_key": ["LOYALTY_TRANSACTION_ID"],
        "rank_key": ["LOYALTY_TRANSACTION_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            AMOUNT,
            CUSTOMER_ID,
            DATE_CREATED,
            DOCUMENT_NO,
            EXPIRED_DATE,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            LOYALTY_CUSTOMER_GROUP_ID,
            LOYALTY_LOCATION_GROUP_ID,
            LOYALTY_PROGRAM_ID,
            LOYALTY_TRANSACTION_EXTID,
            LOYALTY_TRANSACTION_ID,
            POINT_0,
            REWARD_RATE,
            TRANSACTION_DATE,
            TRANSACTION_TYPE,
            TRANS_ID,
            TRANS_LOCATION_ID,
            UPDATE_TIME_,
            VALID_FROM_DATE,
            VALID_TO_DATE
        FROM
            "Vua Nem Joint Stock Company".Administrator.LOYALTY_TRANSACTION
        WHERE
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "CUSTOMER_ID", "type": "INTEGER"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DOCUMENT_NO", "type": "STRING"},
        {"name": "EXPIRED_DATE", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LOYALTY_CUSTOMER_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_LOCATION_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_PROGRAM_ID", "type": "INTEGER"},
        {"name": "LOYALTY_TRANSACTION_EXTID", "type": "STRING"},
        {"name": "LOYALTY_TRANSACTION_ID", "type": "INTEGER"},
        {"name": "POINT_0", "type": "INTEGER"},
        {"name": "REWARD_RATE", "type": "INTEGER"},
        {"name": "TRANSACTION_DATE", "type": "TIMESTAMP"},
        {"name": "TRANSACTION_TYPE", "type": "STRING"},
        {"name": "TRANS_ID", "type": "INTEGER"},
        {"name": "TRANS_LOCATION_ID", "type": "INTEGER"},
        {"name": "UPDATE_TIME_", "type": "TIMESTAMP"},
        {"name": "VALID_FROM_DATE", "type": "TIMESTAMP"},
        {"name": "VALID_TO_DATE", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("LOYALTY_TRANSACTION_ID", Integer),
        Column("AMOUNT", Integer),
        Column("CUSTOMER_ID", Integer),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("DOCUMENT_NO", String),
        Column("EXPIRED_DATE", DateTime(timezone=True)),
        Column("IS_INACTIVE", String),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("LOYALTY_CUSTOMER_GROUP_ID", Integer),
        Column("LOYALTY_LOCATION_GROUP_ID", Integer),
        Column("LOYALTY_PROGRAM_ID", Integer),
        Column("LOYALTY_TRANSACTION_EXTID", String),
        Column("POINT_0", Integer),
        Column("REWARD_RATE", Integer),
        Column("TRANSACTION_DATE", DateTime(timezone=True)),
        Column("TRANSACTION_TYPE", String),
        Column("TRANS_ID", Integer),
        Column("TRANS_LOCATION_ID", Integer),
        Column("UPDATE_TIME_", DateTime(timezone=True)),
        Column("VALID_FROM_DATE", DateTime(timezone=True)),
        Column("VALID_TO_DATE", DateTime(timezone=True)),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
