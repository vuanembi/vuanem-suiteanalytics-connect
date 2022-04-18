from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ORIGINATING_LEADS",
    [
        {"name": "ACCOUNTNUMBER", "type": "STRING"},
        {"name": "ACCOUNT_OWNER_NAME", "type": "STRING"},
        {"name": "ALTEMAIL", "type": "STRING"},
        {"name": "ALTERNATE_CONTACT_ID", "type": "INTEGER"},
        {"name": "ALTPHONE", "type": "STRING"},
        {"name": "AMOUNT_COMPLETE", "type": "INTEGER"},
        {"name": "APPROVE_PERSON", "type": "STRING"},
        {"name": "BANKS_BRANCH", "type": "STRING"},
        {"name": "BANK_ACCOUNT", "type": "STRING"},
        {"name": "BANK_NAME", "type": "STRING"},
        {"name": "BILLADDRESS", "type": "STRING"},
        {"name": "BLOCK_SMS", "type": "STRING"},
        {"name": "BRANCH", "type": "STRING"},
        {"name": "CALCULATED_END", "type": "TIMESTAMP"},
        {"name": "CATEGORY_0", "type": "STRING"},
        {"name": "CITY", "type": "STRING"},
        {"name": "COMMENTS", "type": "STRING"},
        {"name": "COMPANYNAME", "type": "STRING"},
        {"name": "CONSOL_DAYS_OVERDUE", "type": "INTEGER"},
        {"name": "CONSOL_DEPOSIT_BALANCE", "type": "INTEGER"},
        {"name": "CONSOL_DEPOSIT_BALANCE_FOREIGN", "type": "INTEGER"},
        {"name": "CONSOL_OPENBALANCE", "type": "INTEGER"},
        {"name": "CONSOL_OPENBALANCE_FOREIGN", "type": "INTEGER"},
        {"name": "CONSOL_UNBILLED_ORDERS", "type": "INTEGER"},
        {"name": "CONSOL_UNBILLED_ORDERS_FOREIGN", "type": "INTEGER"},
        {"name": "CONVERTED_TO_CONTACT_ID", "type": "INTEGER"},
        {"name": "CONVERTED_TO_ID", "type": "INTEGER"},
        {"name": "COST_ESTIMATE", "type": "INTEGER"},
        {"name": "COUNTRY", "type": "STRING"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "CREDITHOLD", "type": "STRING"},
        {"name": "CREDITLIMIT", "type": "INTEGER"},
        {"name": "CURRENCY_ID", "type": "INTEGER"},
        {"name": "CUSTOMER_ID", "type": "INTEGER"},
        {"name": "CUSTOMER_TYPE_ID", "type": "INTEGER"},
        {"name": "DATE_CLOSED", "type": "TIMESTAMP"},
        {"name": "DATE_CONVSERSION", "type": "TIMESTAMP"},
        {"name": "DATE_FIRST_ORDER", "type": "TIMESTAMP"},
        {"name": "DATE_FIRST_SALE", "type": "TIMESTAMP"},
        {"name": "DATE_GROSS_LEAD", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_ORDER", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_SALE", "type": "TIMESTAMP"},
        {"name": "DATE_LEAD", "type": "TIMESTAMP"},
        {"name": "DATE_OF_BIRTH", "type": "TIMESTAMP"},
        {"name": "DATE_PROSPECT", "type": "TIMESTAMP"},
        {"name": "DATE_UPDATED", "type": "TIMESTAMP"},
        {"name": "DAYS_OVERDUE", "type": "INTEGER"},
        {"name": "DEFAULT_AP_ACCOUNT_ID", "type": "INTEGER"},
        {"name": "DEFAULT_ORDER_PRIORITY", "type": "INTEGER"},
        {"name": "DEFAULT_RECEIVABLES_ACCOUNT_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON", "type": "STRING"},
        {"name": "DEPOSIT_BALANCE", "type": "INTEGER"},
        {"name": "DEPOSIT_BALANCE_FOREIGN", "type": "INTEGER"},
        {"name": "DIC", "type": "STRING"},
        {"name": "EMAIL", "type": "STRING"},
        {"name": "EMPLOYEE_RECORD_REFERENCE_ID", "type": "INTEGER"},
        {"name": "ENTITY_CODE", "type": "STRING"},
        {"name": "EXPECTED_CLOSE", "type": "TIMESTAMP"},
        {"name": "EXPIRATION_DATE_FOR_LOYALTY_S", "type": "TIMESTAMP"},
        {"name": "FAX", "type": "STRING"},
        {"name": "FIRSTNAME", "type": "STRING"},
        {"name": "FIRST_SALE_PERIOD_ID", "type": "INTEGER"},
        {"name": "FIRST_VISIT", "type": "TIMESTAMP"},
        {"name": "FROM_CAMPAIGN", "type": "STRING"},
        {"name": "FROM_LANDING", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "HOME_PHONE", "type": "STRING"},
        {"name": "ICO", "type": "STRING"},
        {"name": "IDENTITY_NO_", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_AGENCY", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_DATE", "type": "TIMESTAMP"},
        {"name": "ISEMAILHTML", "type": "STRING"},
        {"name": "ISEMAILPDF", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "ISRELEASEDCOACHING", "type": "STRING"},
        {"name": "ISTAXABLE", "type": "STRING"},
        {"name": "IS_EXEMPT_TIME", "type": "STRING"},
        {"name": "IS_EXPLICIT_CONVERSION", "type": "STRING"},
        {"name": "IS_JOB", "type": "STRING"},
        {"name": "IS_LEAD__BN_BUN", "type": "STRING"},
        {"name": "IS_PERSON", "type": "STRING"},
        {"name": "IS_PRODUCTIVE_TIME", "type": "STRING"},
        {"name": "IS_UTILIZED_TIME", "type": "STRING"},
        {"name": "JOB_END", "type": "TIMESTAMP"},
        {"name": "JOB_START", "type": "TIMESTAMP"},
        {"name": "JOB_TYPE_ID", "type": "INTEGER"},
        {"name": "LASTNAME", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LAST_SALES_ACTIVITY", "type": "TIMESTAMP"},
        {"name": "LAST_SALE_PERIOD_ID", "type": "INTEGER"},
        {"name": "LAST_VISIT", "type": "TIMESTAMP"},
        {"name": "LEAD_SOURCE_ID", "type": "INTEGER"},
        {"name": "LINE1", "type": "STRING"},
        {"name": "LINE2", "type": "STRING"},
        {"name": "LINE3", "type": "STRING"},
        {"name": "LOGINACCESS", "type": "STRING"},
        {"name": "LOYALTY_ELIGIBILITY", "type": "STRING"},
        {"name": "LOYALTY_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_GROUP_LEVEL", "type": "INTEGER"},
        {"name": "LOYALTY_REMAINING_POINT_VALUE", "type": "INTEGER"},
        {"name": "LOYALTY_SMS_VALUE_PENDING_UPD", "type": "STRING"},
        {"name": "LSA_LINK", "type": "STRING"},
        {"name": "LSA_LINK_NAME", "type": "STRING"},
        {"name": "MIDDLENAME", "type": "STRING"},
        {"name": "MOBILE_PHONE", "type": "STRING"},
        {"name": "MULTIPLE_PRICE_ID", "type": "INTEGER"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NEW_SEGMENT_ID", "type": "INTEGER"},
        {"name": "NGY_TO_LEAD", "type": "TIMESTAMP"},
        {"name": "N__GI_RATING_ID", "type": "INTEGER"},
        {"name": "ONBOARDING_DATE", "type": "TIMESTAMP"},
        {"name": "OPENBALANCE", "type": "INTEGER"},
        {"name": "OPENBALANCE_FOREIGN", "type": "INTEGER"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "PARTNER_ID", "type": "INTEGER"},
        {"name": "PARTNER_RECORD_REFERENCE_ID", "type": "INTEGER"},
        {"name": "PAYMENT_TERMS_ID", "type": "INTEGER"},
        {"name": "PERMISSION_CHECK_PRINT", "type": "STRING"},
        {"name": "PHONE", "type": "STRING"},
        {"name": "PRIMARY_CONTACT_ID", "type": "INTEGER"},
        {"name": "PRINT_ON_CHECK_AS", "type": "STRING"},
        {"name": "PROBABILITY", "type": "INTEGER"},
        {"name": "PROJECTED_END", "type": "TIMESTAMP"},
        {"name": "REFERRER", "type": "STRING"},
        {"name": "RELEASE_REASON_ID", "type": "INTEGER"},
        {"name": "REMINDERDAYS", "type": "INTEGER"},
        {"name": "RENEWAL", "type": "TIMESTAMP"},
        {"name": "RESALENUMBER", "type": "STRING"},
        {"name": "REVENUE_ESTIMATE", "type": "INTEGER"},
        {"name": "REV_REC_FORECAST_RULE_ID", "type": "INTEGER"},
        {"name": "REV_REC_FORECAST_TEMPLATE", "type": "INTEGER"},
        {"name": "SALES_REP_ID", "type": "INTEGER"},
        {"name": "SALES_REP_JOB_TITLE_ID", "type": "INTEGER"},
        {"name": "SALES_TERRITORY_ID", "type": "INTEGER"},
        {"name": "SALUTATION", "type": "STRING"},
        {"name": "SHIPADDRESS", "type": "STRING"},
        {"name": "SHIP_COMPLETE", "type": "STRING"},
        {"name": "SMS_FOR_EXP_DATE", "type": "TIMESTAMP"},
        {"name": "SMS_PROMO_ID", "type": "STRING"},
        {"name": "SMS_SENT_DATE", "type": "TIMESTAMP"},
        {"name": "SOURCES_ID", "type": "INTEGER"},
        {"name": "SPECIAL_DISCOUNT", "type": "INTEGER"},
        {"name": "STATE", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "STATUS_DESCR", "type": "STRING"},
        {"name": "STATUS_PROBABILITY", "type": "INTEGER"},
        {"name": "STATUS_READ_ONLY", "type": "STRING"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TAX_CONTACT_FIRST_NAME", "type": "STRING"},
        {"name": "TAX_CONTACT_ID", "type": "INTEGER"},
        {"name": "TAX_CONTACT_LAST_NAME", "type": "STRING"},
        {"name": "TAX_CONTACT_MIDDLE_NAME", "type": "STRING"},
        {"name": "TAX_ITEM_ID", "type": "INTEGER"},
        {"name": "THIRD_PARTY_ACCT", "type": "STRING"},
        {"name": "THIRD_PARTY_CARRIER", "type": "STRING"},
        {"name": "THIRD_PARTY_COUNTRY", "type": "STRING"},
        {"name": "THIRD_PARTY_ZIP_CODE", "type": "STRING"},
        {"name": "TOP_LEVEL_PARENT_ID", "type": "INTEGER"},
        {"name": "UNBILLED_ORDERS", "type": "INTEGER"},
        {"name": "UNBILLED_ORDERS_FOREIGN", "type": "INTEGER"},
        {"name": "UPDATE_TIMESTAMP", "type": "TIMESTAMP"},
        {"name": "URL", "type": "STRING"},
        {"name": "USE_PERCENT_COMPLETE_OVERRIDE", "type": "STRING"},
        {"name": "VAT_REGISTRATION_NO", "type": "STRING"},
        {"name": "VN_CODE", "type": "STRING"},
        {"name": "WEB_LEAD", "type": "STRING"},
        {"name": "ZIPCODE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            ACCOUNTNUMBER,
            ACCOUNT_OWNER_NAME,
            ALTEMAIL,
            ALTERNATE_CONTACT_ID,
            ALTPHONE,
            AMOUNT_COMPLETE,
            APPROVE_PERSON,
            BANKS_BRANCH,
            BANK_ACCOUNT,
            BANK_NAME,
            BILLADDRESS,
            BLOCK_SMS,
            BRANCH,
            CALCULATED_END,
            CATEGORY_0,
            CITY,
            COMMENTS,
            COMPANYNAME,
            CONSOL_DAYS_OVERDUE,
            CONSOL_DEPOSIT_BALANCE,
            CONSOL_DEPOSIT_BALANCE_FOREIGN,
            CONSOL_OPENBALANCE,
            CONSOL_OPENBALANCE_FOREIGN,
            CONSOL_UNBILLED_ORDERS,
            CONSOL_UNBILLED_ORDERS_FOREIGN,
            CONVERTED_TO_CONTACT_ID,
            CONVERTED_TO_ID,
            COST_ESTIMATE,
            COUNTRY,
            CREATE_DATE,
            CREDITHOLD,
            CREDITLIMIT,
            CURRENCY_ID,
            CUSTOMER_ID,
            CUSTOMER_TYPE_ID,
            DATE_CLOSED,
            DATE_CONVSERSION,
            DATE_FIRST_ORDER,
            DATE_FIRST_SALE,
            DATE_GROSS_LEAD,
            DATE_LAST_MODIFIED,
            DATE_LAST_ORDER,
            DATE_LAST_SALE,
            DATE_LEAD,
            DATE_OF_BIRTH,
            DATE_PROSPECT,
            DATE_UPDATED,
            DAYS_OVERDUE,
            DEFAULT_AP_ACCOUNT_ID,
            DEFAULT_ORDER_PRIORITY,
            DEFAULT_RECEIVABLES_ACCOUNT_ID,
            DELIVERY_PERSON,
            DEPOSIT_BALANCE,
            DEPOSIT_BALANCE_FOREIGN,
            DIC,
            EMAIL,
            EMPLOYEE_RECORD_REFERENCE_ID,
            ENTITY_CODE,
            EXPECTED_CLOSE,
            EXPIRATION_DATE_FOR_LOYALTY_S,
            FAX,
            FIRSTNAME,
            FIRST_SALE_PERIOD_ID,
            FIRST_VISIT,
            FROM_CAMPAIGN,
            FROM_LANDING,
            FULL_NAME,
            HOME_PHONE,
            ICO,
            IDENTITY_NO_,
            IDENTITY_PROVIDED_AGENCY,
            IDENTITY_PROVIDED_DATE,
            ISEMAILHTML,
            ISEMAILPDF,
            ISINACTIVE,
            ISRELEASEDCOACHING,
            ISTAXABLE,
            IS_EXEMPT_TIME,
            IS_EXPLICIT_CONVERSION,
            IS_JOB,
            IS_LEAD__BN_BUN,
            IS_PERSON,
            IS_PRODUCTIVE_TIME,
            IS_UTILIZED_TIME,
            JOB_END,
            JOB_START,
            JOB_TYPE_ID,
            LASTNAME,
            LAST_MODIFIED_DATE,
            LAST_SALES_ACTIVITY,
            LAST_SALE_PERIOD_ID,
            LAST_VISIT,
            LEAD_SOURCE_ID,
            LINE1,
            LINE2,
            LINE3,
            LOGINACCESS,
            LOYALTY_ELIGIBILITY,
            LOYALTY_GROUP_ID,
            LOYALTY_GROUP_LEVEL,
            LOYALTY_REMAINING_POINT_VALUE,
            LOYALTY_SMS_VALUE_PENDING_UPD,
            LSA_LINK,
            LSA_LINK_NAME,
            MIDDLENAME,
            MOBILE_PHONE,
            MULTIPLE_PRICE_ID,
            NAME,
            NEW_SEGMENT_ID,
            NGY_TO_LEAD,
            N__GI_RATING_ID,
            ONBOARDING_DATE,
            OPENBALANCE,
            OPENBALANCE_FOREIGN,
            PARENT_ID,
            PARTNER_ID,
            PARTNER_RECORD_REFERENCE_ID,
            PAYMENT_TERMS_ID,
            PERMISSION_CHECK_PRINT,
            PHONE,
            PRIMARY_CONTACT_ID,
            PRINT_ON_CHECK_AS,
            PROBABILITY,
            PROJECTED_END,
            REFERRER,
            RELEASE_REASON_ID,
            REMINDERDAYS,
            RENEWAL,
            RESALENUMBER,
            REVENUE_ESTIMATE,
            REV_REC_FORECAST_RULE_ID,
            REV_REC_FORECAST_TEMPLATE,
            SALES_REP_ID,
            SALES_REP_JOB_TITLE_ID,
            SALES_TERRITORY_ID,
            SALUTATION,
            SHIPADDRESS,
            SHIP_COMPLETE,
            SMS_FOR_EXP_DATE,
            SMS_PROMO_ID,
            SMS_SENT_DATE,
            SOURCES_ID,
            SPECIAL_DISCOUNT,
            STATE,
            STATUS,
            STATUS_DESCR,
            STATUS_PROBABILITY,
            STATUS_READ_ONLY,
            SUBSIDIARY_ID,
            TAX_CONTACT_FIRST_NAME,
            TAX_CONTACT_ID,
            TAX_CONTACT_LAST_NAME,
            TAX_CONTACT_MIDDLE_NAME,
            TAX_ITEM_ID,
            THIRD_PARTY_ACCT,
            THIRD_PARTY_CARRIER,
            THIRD_PARTY_COUNTRY,
            THIRD_PARTY_ZIP_CODE,
            TOP_LEVEL_PARENT_ID,
            UNBILLED_ORDERS,
            UNBILLED_ORDERS_FOREIGN,
            UPDATE_TIMESTAMP,
            URL,
            USE_PERCENT_COMPLETE_OVERRIDE,
            VAT_REGISTRATION_NO,
            VN_CODE,
            WEB_LEAD,
            ZIPCODE
        FROM
            "Vua Nem Joint Stock Company".Administrator.ORIGINATING_LEADS
        WHERE
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["CUSTOMER_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
