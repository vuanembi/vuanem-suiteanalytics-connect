from sqlalchemy import Column, Integer, String, DateTime, Float

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_promotionCode(NetSuite):
    query = """
        SELECT
            pc.enddate,
            pc.startdate,
            pc.discount,
            pc.description,
            pc.discounttype,
            pc.discounteditemssavedsearch,
            pc.displaylinediscounts,
            pc.externalid,
            pc.fixedprice,
            pc.id,
            pc.isinactive,
            pc.itemquantifier,
            pc.location,
            pc.name,
            pc.promotiontype,
            pc.rate,
            pc.repeatdiscount
        FROM
            promotionCode pc
    """
    schema = [
        {"name": "enddate", "type": "TIMESTAMP"},
        {"name": "startdate", "type": "TIMESTAMP"},
        {"name": "discount", "type": "INTEGER"},
        {"name": "description", "type": "STRING"},
        {"name": "discounttype", "type": "STRING"},
        {"name": "discounteditemssavedsearch", "type": "INTEGER"},
        {"name": "displaylinediscounts", "type": "STRING"},
        {"name": "externalid", "type": "STRING"},
        {"name": "fixedprice", "type": "INTEGER"},
        {"name": "id", "type": "INTEGER"},
        {"name": "isinactive", "type": "STRING"},
        {"name": "itemquantifier", "type": "INTEGER"},
        {"name": "location", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "promotiontype", "type": "STRING"},
        {"name": "rate", "type": "FLOAT"},
        {"name": "repeatdiscount", "type": "STRING"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
    ]
    columns = [
        Column("enddate", DateTime(timezone=True)),
        Column("startdate", DateTime(timezone=True)),
        Column("discount", Integer),
        Column("description", String),
        Column("discounttype", String),
        Column("discounteditemssavedsearch", Integer),
        Column("displaylinediscounts", String),
        Column("externalid", String),
        Column("fixedprice", Integer),
        Column("id", Integer),
        Column("isinactive", String),
        Column("itemquantifier", Integer),
        Column("location", String),
        Column("name", String),
        Column("promotiontype", String),
        Column("rate", Float),
        Column("repeatdiscount", String),
        Column("lastmodifieddate", DateTime(timezone=True)),
    ]
    connector = connector.NetSuite2Connector
    getter = getter.StandardGetter
    loader = [
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
