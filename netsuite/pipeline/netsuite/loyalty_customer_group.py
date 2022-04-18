from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "LOYALTY_CUSTOMER_GROUP",
    [
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
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
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
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["LOYALTY_CUSTOMER_GROUP_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
