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
    NULL AS LOYALTY_ELIGIBILITY,
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
    TRANSACTIONS.SUPPORT_PERSON_ID,
    TRANSACTIONS.TRANSACTION_TYPE,
    TRANSACTIONS.REF__INVOICE__DELIVERY_ORDE_ID,
    TRANSACTIONS.REF__TRANSACTION_ID,
    TRANSACTIONS.RETURNED_REASON_ID,
    TRANSACTIONS.REFERENCE_SALES_ORDER_ID,
    TRANSACTIONS.TRANSFER_LOCATION,
    TRANSACTIONS.TRANSFER_ORDER_TYPE_ID,
    TRANSACTIONS.PURCHASE_ORDER_TYPE_ID
FROM
    "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTIONS
WHERE
    TRANSACTIONS.DATE_LAST_MODIFIED >= ?
    AND TRANSACTIONS.DATE_LAST_MODIFIED <= ?
