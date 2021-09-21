from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class PROMOTION_SMS_INTEGRATION(NetSuite):
    keys = {
        "p_key": ["PROMOTION_SMS_INTEGRATION_ID"],
        "rank_key": ["PROMOTION_SMS_INTEGRATION_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
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
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
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
    ]
    columns = [
        Column("PROMOTION_SMS_INTEGRATION_ID", Integer),
        Column("COUPON_CODES", String),
        Column("CUSTOMER_ID", Integer),
        Column("CUSTOMER_NAME", String),
        Column("CUSTOMER_NUMBER", String),
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("END_DATE", String),
        Column("INTERNAL_ID_COUPON_CODE", Integer),
        Column("ISLOCK", String),
        Column("ISSEND", String),
        Column("IS_INACTIVE", String),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("MESSENGER_TEMPLATE", String),
        Column("PARENT_ID", Integer),
        Column("PHONE_NUMBER", String),
        Column("PROMOTION_INTERNAL_ID", Integer),
        Column("PROMOTION_NAME", String),
        Column("PROMOTION_SMS_INTEGRATION_EXTI", String),
        Column("SEND_TIME", String),
        Column("SEND_TIME_DATETIME", DateTime(timezone=True)),
        Column("START_DATE", String),
        Column("STATUS_SEND", String),
        Column("VERIFY_SMS", String),
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]
