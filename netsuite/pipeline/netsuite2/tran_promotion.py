from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ns2_tranPromotion",
    [
        {"name": "couponcode", "type": "INTEGER"},
        {"name": "eligiblefreegifts", "type": "FLOAT"},
        {"name": "freegiftsadded", "type": "FLOAT"},
        {"name": "promocode", "type": "INTEGER"},
        {"name": "promotiontype", "type": "STRING"},
        {"name": "purchasediscount", "type": "FLOAT"},
        {"name": "shippingdiscount", "type": "FLOAT"},
        {"name": "transaction", "type": "INTEGER"},
        {"name": "applicabilitystatus", "type": "STRING"},
        {"name": "lastmodifieddate", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda tr: f"""
        SELECT 
            a.couponcode,
            a.eligiblefreegifts,
            a.freegiftsadded,
            a.promocode,
            a.promotiontype,
            a.purchasediscount,
            a.shippingdiscount,
            a.transaction,
            a.applicabilitystatus,
            b.lastmodifieddate
        FROM
            tranPromotion a
        LEFT JOIN transaction b
        ON b.id = a.transaction
        WHERE
            b.lastmodifieddate >= TO_DATE('{tr[0]}', 'YYYY-MM-DD HH24:MI:SS')
            AND b.lastmodifieddate <= TO_DATE('{tr[1]}','YYYY-MM-DD HH24:MI:SS')
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["transaction", "couponcode", "promocode"],
        rank_key=["transaction"],
        cursor_key=["lastmodifieddate"],
        cursor_rank_key=["lastmodifieddate"],
        cursor_rn_key=["lastmodifieddate"],
    ),
    load_callback_fn=update,
)
