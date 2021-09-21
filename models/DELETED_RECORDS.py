from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class DELETED_RECORDS(NetSuite):
    keys = {
        "p_key": ["RECORD_ID"],
        "rank_key": ["RECORD_ID"],
        "incre_key": ["DATE_DELETED"],
        "rank_incre_key": ["DATE_DELETED"],
        "row_num_incre_key": ["DATE_DELETED"],
    }
    query = """
        SELECT
            CUSTOM_RECORD_TYPE,
            DATE_DELETED,
            ENTITY_ID,
            ENTITY_NAME,
            IS_CUSTOM_LIST,
            NAME,
            RECORD_BASE_TYPE,
            RECORD_ID,
            RECORD_TYPE_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.DELETED_RECORDS
        WHERE
            DATE_DELETED >= '{{ start }}'
            AND DATE_DELETED <= '{{ end }}'
    """
    schema = [
        {"name": "CUSTOM_RECORD_TYPE", "type": "STRING"},
        {"name": "DATE_DELETED", "type": "TIMESTAMP"},
        {"name": "ENTITY_ID", "type": "INTEGER"},
        {"name": "ENTITY_NAME", "type": "STRING"},
        {"name": "IS_CUSTOM_LIST", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "RECORD_BASE_TYPE", "type": "STRING"},
        {"name": "RECORD_ID", "type": "INTEGER"},
        {"name": "RECORD_TYPE_NAME", "type": "STRING"},
    ]
    columns = [
        Column("CUSTOM_RECORD_TYPE", String),
        Column("DATE_DELETED", DateTime(timezone=True)),
        Column("ENTITY_ID", Integer),
        Column("ENTITY_NAME", String),
        Column("IS_CUSTOM_LIST", String),
        Column("NAME", String),
        Column("RECORD_BASE_TYPE", String),
        Column("RECORD_ID", Integer),
        Column("RECORD_TYPE_NAME", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]
