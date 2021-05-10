from pipelines import NetSuiteIncrementalJob


class TRANSACTION_LINES(NetSuiteIncrementalJob):
    query = """
    SELECT
        TRANSACTION_LINES.TRANSACTION_ID,
        TRANSACTION_LINES.TRANSACTION_LINE_ID,
        TRANSACTION_LINES.ACCOUNT_ID,
        TRANSACTION_LINES.AMOUNT,
        TRANSACTION_LINES.AMOUNT_BEFORE_DISCOUNT,
        TRANSACTION_LINES.AMOUNT_FOREIGN_LINKED,
        TRANSACTION_LINES.CLASS_ID,
        TRANSACTION_LINES.COMPANY_ID,
        TRANSACTION_LINES.DATE_CLOSED,
        TRANSACTION_LINES.DATE_CREATED,
        TRANSACTION_LINES.DATE_LAST_MODIFIED,
        TRANSACTION_LINES.DELIVERY_METHOD_ID,
        TRANSACTION_LINES.DELIVER_LOCATION_ID,
        TRANSACTION_LINES.DEPARTMENT_ID,
        TRANSACTION_LINES.EXPECTED_DELIVERY_DATE_SO,
        TRANSACTION_LINES.GROSS_AMOUNT,
        TRANSACTION_LINES.IS_COST_LINE,
        TRANSACTION_LINES.ITEM_COUNT,
        TRANSACTION_LINES.ITEM_GROUP_PROMOTION_ID,
        TRANSACTION_LINES.ITEM_ID,
        TRANSACTION_LINES.ITEM_TYPE,
        TRANSACTION_LINES.ITEM_UNIT_PRICE,
        TRANSACTION_LINES.LOCATION_ID,
        TRANSACTION_LINES.MEMO,
        TRANSACTION_LINES.NET_AMOUNT,
        TRANSACTION_LINES.SUBSIDIARY_ID,
        TRANSACTION_LINES.QUANTITY_RECEIVED_IN_SHIPMENT,
        TRANSACTION_LINES.TRANSFER_ORDER_ITEM_LINE,
        TRANSACTION_LINES.TRANSFER_ORDER_LINE_TYPE,
        TRANSACTION_LINES.TRANSACTION_ORDER,
        TRANSACTION_LINES.VENDOR_ID
    FROM
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTION_LINES
    WHERE
        TRANSACTION_LINES.DATE_LAST_MODIFIED > ?
    """

    schema = [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_LINE_ID", "type": "INTEGER"},
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "AMOUNT", "type": "INTEGER"},
        {"name": "AMOUNT_BEFORE_DISCOUNT", "type": "INTEGER"},
        {"name": "CLASS_ID", "type": "INTEGER"},
        {"name": "COMPANY_ID", "type": "INTEGER"},
        {"name": "DATE_CLOSED", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DELIVERY_METHOD_ID", "type": "INTEGER"},
        {"name": "DELIVER_LOCATION_ID", "type": "INTEGER"},
        {"name": "DEPARTMENT_ID", "type": "INTEGER"},
        {"name": "EXPECTED_DELIVERY_DATE_SO", "type": "TIMESTAMP"},
        {"name": "GROSS_AMOUNT", "type": "INTEGER"},
        {"name": "IS_COST_LINE", "type": "STRING"},
        {"name": "ITEM_COUNT", "type": "INTEGER"},
        {"name": "ITEM_GROUP_PROMOTION_ID", "type": "INTEGER"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "ITEM_TYPE", "type": "STRING"},
        {"name": "ITEM_UNIT_PRICE", "type": "STRING"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "MEMO", "type": "STRING"},
        {"name": "NET_AMOUNT", "type": "INTEGER"},
        {"name": "QUANTITY_RECEIVED_IN_SHIPMENT", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TRANSACTION_ORDER", "type": "INTEGER"},
        {"name": "TRANSFER_ORDER_ITEM_LINE", "type": "INTEGER"},
        {"name": "TRANSFER_ORDER_LINE_TYPE", "type": "STRING"},
        {"name": "VENDOR_ID", "type": "INTEGER"},
    ]
