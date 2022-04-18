from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "STORE_TRAFFIC",
    [
        {"name": "DATE_0", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "GENDER_ID", "type": "INTEGER"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STORE_TRAFFIC_ID", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TOTAL_TIMES_OF_VISITING", "type": "INTEGER"},
        {"name": "TOTAL_VISITOR", "type": "INTEGER"},
        {"name": "AVERAGE_NO__OF_ITEMS_PER_SALE", "type": "INTEGER"},
        {"name": "AVERAGE_VALUE_PER_SALES_ORDER", "type": "INTEGER"},
        {"name": "CONVERTER_RATE", "type": "INTEGER"},
        {"name": "EMPLOYEE_REFERENCE_ID", "type": "INTEGER"},
        {"name": "NO__OF_SALES_ORDER", "type": "INTEGER"},
        {"name": "NO__OF_SELLING_ITEMS", "type": "INTEGER"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "STORE_TRAFFIC_EXTID", "type": "STRING"},
        {"name": "TIME_SLOT_ID", "type": "INTEGER"},
        {"name": "TOTAL_SALES_ORDER_VALUE", "type": "INTEGER"},
        {"name": "TRAFFIC_RATE", "type": "INTEGER"},
        {"name": "TRAFFIC_SOURCES_ID", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            DATE_0,
            DATE_CREATED,
            GENDER_ID,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            LOCATION_ID,
            STORE_TRAFFIC_ID,
            SUBSIDIARY_ID,
            TOTAL_TIMES_OF_VISITING,
            TOTAL_VISITOR,
            AVERAGE_NO__OF_ITEMS_PER_SALE,
            AVERAGE_VALUE_PER_SALES_ORDER,
            CONVERTER_RATE,
            EMPLOYEE_REFERENCE_ID,
            NO__OF_SALES_ORDER,
            NO__OF_SELLING_ITEMS,
            PARENT_ID,
            STORE_TRAFFIC_EXTID,
            SUBSIDIARY_ID,
            TIME_SLOT_ID,
            TOTAL_SALES_ORDER_VALUE,
            TRAFFIC_RATE,
            TRAFFIC_SOURCES_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.STORE_TRAFFIC
        WHERE
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["STORE_TRAFFIC_ID"],
        rank_key=["STORE_TRAFFIC_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
        cursor_rank_key=["LAST_MODIFIED_DATE"],
        cursor_rn_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
