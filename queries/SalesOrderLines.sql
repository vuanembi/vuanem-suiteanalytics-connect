SELECT
    TRANSACTION_LINES.TRANSACTION_ID,
    TRANSACTION_LINES.LOCATION_ID,
    TRANSACTION_LINES.ITEM_COUNT,
    TRANSACTION_LINES.NET_AMOUNT,
    TRANSACTIONS.TRANDATE,
    TRANSACTIONS.TRANID,
    TRANSACTIONS.CUSTOMER_PHONE,
    TRANSACTIONS.STATUS,
    CLASSES.PRODUCT_GROUP_CODE,
    CLASSES.FULL_NAME
FROM
    (
        SELECT
            TRANSACTION_LINES.TRANSACTION_ID,
            TRANSACTION_LINES.CLASS_ID,
            TRANSACTION_LINES.ITEM_ID,
            TRANSACTION_LINES.LOCATION_ID,
            - TRANSACTION_LINES.ITEM_COUNT,
            - TRANSACTION_LINES.NET_AMOUNT
        FROM
            "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTION_LINES
        WHERE
            TRANSACTION_LINES.ACCOUNT_ID IN (
                480,
                482,
                487,
                498,
                505,
                508,
                509,
                510,
                511,
                54,
                1079,
                1170
            )
    ) TRANSACTION_LINES
    INNER JOIN (
        SELECT
            TRANSACTIONS.TRANSACTION_ID,
            TRANSACTIONS.TRANDATE,
            TRANSACTIONS.TRANID,
            TRANSACTIONS.CUSTOMER_PHONE,
            TRANSACTIONS.STATUS
        FROM
            "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".TRANSACTIONS
        WHERE
            TRANSACTIONS.TRANSACTION_TYPE = 'Sales Order'
            AND TRANSACTIONS.STATUS <> 'Closed'
    ) TRANSACTIONS ON TRANSACTION_LINES.TRANSACTION_ID = TRANSACTIONS.TRANSACTION_ID
    INNER JOIN (
        SELECT
            CLASSES.CLASS_ID,
            CLASSES.PRODUCT_GROUP_CODE,
            CLASSES.FULL_NAME
        FROM
            "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".CLASSES
    ) CLASSES ON CLASSES.CLASS_ID = TRANSACTION_LINES.CLASS_ID
WHERE
    TRANSACTION_LINES.ITEM_ID IN (
        SELECT
            INVENTORY_ITEMS.ITEM_ID
        FROM
            "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".INVENTORY_ITEMS
        WHERE
            INVENTORY_ITEMS.DISPLAYNAME IS NOT NULL
    )
