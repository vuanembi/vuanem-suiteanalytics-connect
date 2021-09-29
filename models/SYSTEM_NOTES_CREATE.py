from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class SYSTEM_NOTES_CREATE(NetSuite):
    keys = {
        "p_key": ["RECORD_ID", "RECORD_TYPE_ID", "LINE_ID", "LINE_TRANSACTION_ID"],
        "rank_key": ["RECORD_ID", "RECORD_TYPE_ID", "LINE_ID", "LINE_TRANSACTION_ID"],
        "incre_key": ["DATE_CREATED"],
        "rank_incre_key": ["DATE_CREATED"],
        "row_num_incre_key": ["DATE_CREATED"],
    }
    query = """
        SELECT
            AUTHOR_ID,
            COMPANY_ID,
            CONTEXT_TYPE_NAME,
            CUSTOM_FIELD,
            DATE_CREATED,
            EVENT_ID,
            ITEM_ID,
            LINE_ID,
            LINE_TRANSACTION_ID,
            NAME,
            NOTE_TYPE_ID,
            OPERATION,
            RECORD_ID,
            RECORD_TYPE_ID,
            ROLE_ID,
            TRANSACTION_ID,
            VALUE_NEW,
            VALUE_OLD
        FROM
            "Vua Nem Joint Stock Company"."Administrator".SYSTEM_NOTES
        WHERE
            (
                DATE_CREATED >= '{{ start }}'
                AND DATE_CREATED <= '{{ end }}'
            )
            AND OPERATION LIKE '%Create%'
    """
    schema = [
        {"name": "AUTHOR_ID", "type": "INTEGER"},
        {"name": "COMPANY_ID", "type": "INTEGER"},
        {"name": "CONTEXT_TYPE_NAME", "type": "STRING"},
        {"name": "CUSTOM_FIELD", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "EVENT_ID", "type": "INTEGER"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "LINE_ID", "type": "INTEGER"},
        {"name": "LINE_TRANSACTION_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NOTE_TYPE_ID", "type": "INTEGER"},
        {"name": "OPERATION", "type": "STRING"},
        {"name": "RECORD_ID", "type": "INTEGER"},
        {"name": "RECORD_TYPE_ID", "type": "INTEGER"},
        {"name": "ROLE_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "VALUE_NEW", "type": "STRING"},
        {"name": "VALUE_OLD", "type": "STRING"},
    ]
    columns = [
        Column("AUTHOR_ID", Integer),
        Column("COMPANY_ID", Integer),
        Column("CONTEXT_TYPE_NAME", String),
        Column("CUSTOM_FIELD", String),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("EVENT_ID", Integer),
        Column("ITEM_ID", Integer),
        Column("LINE_ID", Integer),
        Column("LINE_TRANSACTION_ID", Integer),
        Column("NAME", String),
        Column("NOTE_TYPE_ID", Integer),
        Column("OPERATION", String),
        Column("RECORD_ID", Integer),
        Column("RECORD_TYPE_ID", Integer),
        Column("ROLE_ID", Integer),
        Column("TRANSACTION_ID", Integer),
        Column("VALUE_NEW", String),
        Column("VALUE_OLD", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]
