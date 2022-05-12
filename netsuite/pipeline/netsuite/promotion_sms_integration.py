from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "PROMOTION_SMS_INTEGRATION",
    [
        {"name": "PROMOTION_SMS_INTEGRATION_ID", "type": "INTEGER"},
        {"name": "COUPON_CODES", "type": "STRING"},
        {"name": "CUSTOMER_ID", "type": "INTEGER"},
        {"name": "CUSTOMER_NAME", "type": "STRING"},
        {"name": "CUSTOMER_NUMBER", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "END_DATE", "type": "STRING"},
        {"name": "INTERNAL_ID_COUPON_CODE", "type": "INTEGER"},
        {"name": "ISLOCK", "type": "STRING"},
        {"name": "ISSEND", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "MESSENGER_TEMPLATE", "type": "STRING"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "PHONE_NUMBER", "type": "STRING"},
        {"name": "PROMOTION_INTERNAL_ID", "type": "INTEGER"},
        {"name": "PROMOTION_NAME", "type": "STRING"},
        {"name": "PROMOTION_SMS_INTEGRATION_EXTI", "type": "STRING"},
        {"name": "SEND_TIME", "type": "STRING"},
        {"name": "SEND_TIME_DATETIME", "type": "TIMESTAMP"},
        {"name": "START_DATE", "type": "STRING"},
        {"name": "STATUS_SEND", "type": "STRING"},
        {"name": "VERIFY_SMS", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            PROMOTION_SMS_INTEGRATION_ID,
            COUPON_CODES,
            CUSTOMER_ID,
            CUSTOMER_NAME,
            CUSTOMER_NUMBER,
            DATE_CREATED,
            END_DATE,
            INTERNAL_ID_COUPON_CODE,
            ISLOCK,
            ISSEND,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            MESSENGER_TEMPLATE,
            PARENT_ID,
            PHONE_NUMBER,
            PROMOTION_INTERNAL_ID,
            PROMOTION_NAME,
            PROMOTION_SMS_INTEGRATION_EXTI,
            SEND_TIME,
            SEND_TIME_DATETIME,
            START_DATE,
            STATUS_SEND,
            VERIFY_SMS
        FROM
            "Vua Nem Joint Stock Company".Administrator.PROMOTION_SMS_INTEGRATION
        WHERE
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["PROMOTION_SMS_INTEGRATION_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
