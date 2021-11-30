from sqlalchemy import Column, Integer, String, DateTime, Float

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_tranPromotion(NetSuite):
    keys = {
        "p_key": ["transaction", "couponcode", "promocode"],
        "rank_key": ["transaction"],
        "incre_key": ["lastmodifieddate"],
        "rank_incre_key": ["lastmodifieddate"],
        "row_num_incre_key": ["lastmodifieddate"],
    }
    query = """
        SELECT
            a.couponcode,
            a.eligiblefreegifts,
            a.freegiftsadded,
            a.promocode,
            a.promotiontype,
            a.purchasediscount,
            a.shippingdiscount,
            a.transaction,
            b.lastmodifieddate
        FROM
            tranPromotion a
            LEFT JOIN transaction b ON b.id = a.transaction
        WHERE
            b.lastmodifieddate >= TO_DATE(
                '{{ start }}',
                'YYYY-MM-DD HH24:MI:SS'
            )
            AND b.lastmodifieddate <= TO_DATE(
                '{{ end }}',
                'YYYY-MM-DD HH24:MI:SS'
            )
    """
    schema = [
        {"name": "couponcode", "type": "INTEGER"},
        {"name": "eligiblefreegifts", "type": "FLOAT"},
        {"name": "freegiftsadded", "type": "FLOAT"},
        {"name": "promocode", "type": "INTEGER"},
        {"name": "promotiontype", "type": "STRING"},
        {"name": "purchasediscount", "type": "FLOAT"},
        {"name": "shippingdiscount", "type": "FLOAT"},
        {"name": "transaction", "type": "INTEGER"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("transaction", Integer),
        Column("couponcode", Integer),
        Column("eligiblefreegifts", Float),
        Column("freegiftsadded", Float),
        Column("promocode", Integer),
        Column("promotiontype", String),
        Column("purchasediscount", Float),
        Column("shippingdiscount", Float),
        Column("lastmodifieddate", DateTime(timezone=True)),
    ]
    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
