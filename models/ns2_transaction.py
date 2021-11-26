from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_transaction(NetSuite):
    keys = {
        "p_key": ["id"],
        "rank_key": ["id"],
        "incre_key": ["lastmodifieddate"],
        "rank_incre_key": ["lastmodifieddate"],
        "row_num_incre_key": ["lastmodifieddate"],
    }
    query = """
        SELECT
            id,
            lastmodifieddate,
            recordtype
        FROM
            transaction
        WHERE
            lastmodifieddate >= TO_DATE('{{ start }}', 'YYYY-MM-DD HH24:MI:SS')
            AND lastmodifieddate <= TO_DATE('{{ end }}', 'YYYY-MM-DD HH24:MI:SS')
    """
    schema = [
        {"name": "id", "type": "INTEGER"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
        {"name": "recordtype", "type": "STRING"},
    ]
    columns = [
        Column("code", String),
        Column("datesent", DateTime(timezone=True)),
        Column("externalid", String),
        Column("id", Integer),
        Column("promotion", Integer),
    ]
    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
