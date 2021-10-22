from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_couponCode(NetSuite):
    keys = {
        "p_key": ["id"],
        "rank_key": ["id"],
        "incre_key": ["id"],
        "rank_incre_key": ["id"],
        "row_num_incre_key": ["id"],
    }
    query = """
        SELECT
            code,
            datesent,
            externalid,
            id,
            promotion
        FROM
            couponCode cc
        WHERE
            id >= {{ start }}
            AND id <= {{ end }}
    """
    schema = [
        {"name": "code", "type": "STRING"},
        {"name": "datesent", "type": "TIMESTAMP"},
        {"name": "externalid", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "promotion", "type": "INTEGER"},
    ]
    columns = [
        Column("code", String),
        Column("datesent", DateTime(timezone=True)),
        Column("externalid", String),
        Column("id", Integer, primary_key=True),
        Column("promotion", Integer),
    ]
    connector = connector.NetSuite2Connector
    getter = getter.IDIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
