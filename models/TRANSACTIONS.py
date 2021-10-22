from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class TRANSACTIONS(NetSuite):
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            TRANSACTIONS.TRANSACTION_ID,
            TRANSACTIONS.TRANSACTION_NUMBER,
            TRANSACTIONS.TRANDATE,
            TRANSACTIONS.TRANID,
            TRANSACTIONS.BILLADDRESS,
            TRANSACTIONS.CLOSED,
            TRANSACTIONS.CLOSED_REASON_ID,
            TRANSACTIONS.CREATE_DATE,
            TRANSACTIONS.CREATED_FROM_ID,
            TRANSACTIONS.CUSTOMER_PHONE,
            TRANSACTIONS.DATE_LAST_MODIFIED,
            TRANSACTIONS.DELIVERY_PERSON_ID,
            TRANSACTIONS.DELIVERY_STATUS_ID,
            TRANSACTIONS.DELIVERY_TERMS_ID,
            TRANSACTIONS.DELIVERY_VEHICLE_ID,
            TRANSACTIONS.DEPOSIT_PAYMENT_METHOD_ID,
            TRANSACTIONS.EINVOICE_STATUS,
            TRANSACTIONS.EMAIL,
            TRANSACTIONS.ENTITY_ID,
            TRANSACTIONS.EVENT_ID,
            TRANSACTIONS.EXPECTED_DELIVERY_DATE_C,
            TRANSACTIONS.IN_CHARGE_LOCATION_ID,
            TRANSACTIONS.KHNG_YU_CU_T_CC,
            TRANSACTIONS.LEAD_SOURCE_ID,
            TRANSACTIONS.LICENSE_PLATE,
            TRANSACTIONS.LICENSE_PLATE_V_2_ID,
            TRANSACTIONS.LOCATION_ID,
            TRANSACTIONS.LOYALTY_CUSTOMER_GROUP_ID,
            TRANSACTIONS.LOYALTY_ELIGIBILITY,
            TRANSACTIONS.LOYALTY_LOCATION_GROUP_ID,
            TRANSACTIONS.MAGENTO_ORDER_NUMBER,
            TRANSACTIONS.MAGENTO_SO_ID,
            TRANSACTIONS.MAGENTO_TOTAL,
            TRANSACTIONS.MEMO,
            TRANSACTIONS.ORDER_PAYMENT_METHOD_ID,
            TRANSACTIONS.PARTNER_ID,
            TRANSACTIONS.PROMOTION_CODE_ID,
            TRANSACTIONS.RECIPIENT,
            TRANSACTIONS.RECIPIENT_PHONE,
            TRANSACTIONS.SALES_REP_ID,
            TRANSACTIONS.SHIPADDRESS,
            TRANSACTIONS.SHIPPING_METHOD_C_ID,
            TRANSACTIONS.STATUS,
            TRANSACTIONS.TRANSACTION_TYPE,
            TRANSACTIONS.REF__INVOICE__DELIVERY_ORDE_ID,
            TRANSACTIONS.REF__TRANSACTION_ID,
            TRANSACTIONS.RETURNED_REASON_ID,
            TRANSACTIONS.REFERENCE_SALES_ORDER_ID,
            TRANSACTIONS.TRANSFER_LOCATION,
            TRANSACTIONS.TRANSFER_ORDER_TYPE_ID,
            TRANSACTIONS.PURCHASE_ORDER_TYPE_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.TRANSACTIONS
        WHERE
            TRANSACTIONS.DATE_LAST_MODIFIED >= '{{ start }}'
            AND TRANSACTIONS.DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
        {"name": "TRANID", "type": "STRING"},
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_NUMBER", "type": "STRING"},
        {"name": "TRANDATE", "type": "TIMESTAMP"},
        {"name": "BILLADDRESS", "type": "STRING"},
        {"name": "CLOSED", "type": "TIMESTAMP"},
        {"name": "CLOSED_REASON_ID", "type": "INTEGER"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "CREATED_FROM_ID", "type": "INTEGER"},
        {"name": "CUSTOMER_PHONE", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_PERSON_ID", "type": "INTEGER"},
        {"name": "DELIVERY_STATUS_ID", "type": "INTEGER"},
        {"name": "DELIVERY_TERMS_ID", "type": "INTEGER"},
        {"name": "DELIVERY_VEHICLE_ID", "type": "INTEGER"},
        {"name": "DEPOSIT_PAYMENT_METHOD_ID", "type": "INTEGER"},
        {"name": "EINVOICE_STATUS", "type": "STRING"},
        {"name": "EMAIL", "type": "STRING"},
        {"name": "ENTITY_ID", "type": "INTEGER"},
        {"name": "EVENT_ID", "type": "INTEGER"},
        {"name": "EXPECTED_DELIVERY_DATE_C", "type": "TIMESTAMP"},
        {"name": "IN_CHARGE_LOCATION_ID", "type": "INTEGER"},
        {"name": "KHNG_YU_CU_T_CC", "type": "STRING"},
        {"name": "LEAD_SOURCE_ID", "type": "INTEGER"},
        {"name": "LICENSE_PLATE", "type": "STRING"},
        {"name": "LICENSE_PLATE_V_2_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "LOYALTY_CUSTOMER_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_ELIGIBILITY", "type": "STRING"},
        {"name": "LOYALTY_LOCATION_GROUP_ID", "type": "INTEGER"},
        {"name": "MAGENTO_ORDER_NUMBER", "type": "STRING"},
        {"name": "MAGENTO_SO_ID", "type": "STRING"},
        {"name": "MAGENTO_TOTAL", "type": "INTEGER"},
        {"name": "MEMO", "type": "STRING"},
        {"name": "ORDER_PAYMENT_METHOD_ID", "type": "INTEGER"},
        {"name": "PARTNER_ID", "type": "INTEGER"},
        {"name": "PROMOTION_CODE_ID", "type": "STRING"},
        {"name": "RECIPIENT", "type": "STRING"},
        {"name": "RECIPIENT_PHONE", "type": "STRING"},
        {"name": "SALES_REP_ID", "type": "INTEGER"},
        {"name": "SHIPADDRESS", "type": "STRING"},
        {"name": "SHIPPING_METHOD_C_ID", "type": "INTEGER"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "TRANSACTION_TYPE", "type": "STRING"},
        {"name": "REF__INVOICE__DELIVERY_ORDE_ID", "type": "INTEGER"},
        {"name": "REF__TRANSACTION_ID", "type": "INTEGER"},
        {"name": "RETURNED_REASON_ID", "type": "INTEGER"},
        {"name": "REFERENCE_SALES_ORDER_ID", "type": "INTEGER"},
        {"name": "TRANSFER_LOCATION", "type": "INTEGER"},
        {"name": "TRANSFER_ORDER_TYPE_ID", "type": "INTEGER"},
        {"name": "PURCHASE_ORDER_TYPE_ID", "type": "INTEGER"},
    ]
    columns = [
        Column("TRANSACTION_ID", Integer, primary_key=True),
        Column("TRANID", String),
        Column("TRANSACTION_NUMBER", String),
        Column("TRANDATE", DateTime(timezone=True)),
        Column("BILLADDRESS", String),
        Column("CLOSED", DateTime(timezone=True)),
        Column("CLOSED_REASON_ID", Integer),
        Column("CREATE_DATE", DateTime(timezone=True)),
        Column("CREATED_FROM_ID", Integer),
        Column("CUSTOMER_PHONE", String),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("DELIVERY_PERSON_ID", Integer),
        Column("DELIVERY_STATUS_ID", Integer),
        Column("DELIVERY_TERMS_ID", Integer),
        Column("DELIVERY_VEHICLE_ID", Integer),
        Column("DEPOSIT_PAYMENT_METHOD_ID", Integer),
        Column("EINVOICE_STATUS", String),
        Column("EMAIL", String),
        Column("ENTITY_ID", Integer),
        Column("EVENT_ID", Integer),
        Column("EXPECTED_DELIVERY_DATE_C", DateTime(timezone=True)),
        Column("IN_CHARGE_LOCATION_ID", Integer),
        Column("KHNG_YU_CU_T_CC", String),
        Column("LEAD_SOURCE_ID", Integer),
        Column("LICENSE_PLATE", String),
        Column("LICENSE_PLATE_V_2_ID", Integer),
        Column("LOCATION_ID", Integer),
        Column("LOYALTY_CUSTOMER_GROUP_ID", Integer),
        Column("LOYALTY_ELIGIBILITY", String),
        Column("LOYALTY_LOCATION_GROUP_ID", Integer),
        Column("MAGENTO_ORDER_NUMBER", String),
        Column("MAGENTO_SO_ID", String),
        Column("MAGENTO_TOTAL", Integer),
        Column("MEMO", String),
        Column("ORDER_PAYMENT_METHOD_ID", Integer),
        Column("PARTNER_ID", Integer),
        Column("PROMOTION_CODE_ID", String),
        Column("RECIPIENT", String),
        Column("RECIPIENT_PHONE", String),
        Column("SALES_REP_ID", Integer),
        Column("SHIPADDRESS", String),
        Column("SHIPPING_METHOD_C_ID", Integer),
        Column("STATUS", String),
        Column("TRANSACTION_TYPE", String),
        Column("REF__INVOICE__DELIVERY_ORDE_ID", Integer),
        Column("REF__TRANSACTION_ID", Integer),
        Column("RETURNED_REASON_ID", Integer),
        Column("REFERENCE_SALES_ORDER_ID", Integer),
        Column("TRANSFER_LOCATION", Integer),
        Column("TRANSFER_ORDER_TYPE_ID", Integer),
        Column("PURCHASE_ORDER_TYPE_ID", Integer),
    ]
    materialized_view = "NetSuite__Customers2"
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
