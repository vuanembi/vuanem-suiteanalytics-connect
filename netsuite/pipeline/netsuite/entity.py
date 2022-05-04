from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "ENTITY",
    [
        {"name": "ACCOUNT_OWNER_NAME", "type": "STRING"},
        {"name": "ADDRESS_ONE", "type": "STRING"},
        {"name": "ADDRESS_THREE", "type": "STRING"},
        {"name": "ADDRESS_TWO", "type": "STRING"},
        {"name": "APPROVE_PERSON", "type": "STRING"},
        {"name": "BANKS_BRANCH", "type": "STRING"},
        {"name": "BANK_ACCOUNT", "type": "STRING"},
        {"name": "BANK_NAME", "type": "STRING"},
        {"name": "BLOCK_SMS", "type": "STRING"},
        {"name": "BRANCH", "type": "STRING"},
        {"name": "CITY", "type": "STRING"},
        {"name": "COUNTRY", "type": "STRING"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DATE_OF_BIRTH", "type": "TIMESTAMP"},
        {"name": "DATE_UPDATED", "type": "TIMESTAMP"},
        {"name": "DEFAULT_AP_ACCOUNT_ID", "type": "INTEGER"},
        {"name": "DELIVERY_PERSON", "type": "STRING"},
        {"name": "DIC", "type": "STRING"},
        {"name": "EMAIL", "type": "STRING"},
        {"name": "EMPLOYEE_RECORD_REFERENCE_ID", "type": "INTEGER"},
        {"name": "ENTITY_CODE", "type": "STRING"},
        {"name": "ENTITY_EXTID", "type": "STRING"},
        {"name": "ENTITY_ID", "type": "INTEGER"},
        {"name": "ENTITY_TYPE", "type": "STRING"},
        {"name": "EXPIRATION_DATE_FOR_LOYALTY_S", "type": "TIMESTAMP"},
        {"name": "FIRST_NAME", "type": "STRING"},
        {"name": "FROM_CAMPAIGN", "type": "STRING"},
        {"name": "FROM_LANDING", "type": "STRING"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "GLOBAL_SUBSCRIPTION_STATUS", "type": "INTEGER"},
        {"name": "ICO", "type": "STRING"},
        {"name": "IDENTITY_NO_", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_AGENCY", "type": "STRING"},
        {"name": "IDENTITY_PROVIDED_DATE", "type": "TIMESTAMP"},
        {"name": "ISRELEASEDCOACHING", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "IS_LEAD__BN_BUN", "type": "STRING"},
        {"name": "IS_ONLINE_BILL_PAY", "type": "STRING"},
        {"name": "IS_UNAVAILABLE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LAST_NAME", "type": "STRING"},
        {"name": "LAST_SALES_ACTIVITY", "type": "TIMESTAMP"},
        {"name": "LOGIN_ACCESS", "type": "STRING"},
        {"name": "LOYALTY_ELIGIBILITY", "type": "STRING"},
        {"name": "LOYALTY_GROUP_ID", "type": "INTEGER"},
        {"name": "LOYALTY_GROUP_LEVEL", "type": "INTEGER"},
        {"name": "LOYALTY_REMAINING_POINT_VALUE", "type": "INTEGER"},
        {"name": "LOYALTY_SMS_VALUE_PENDING_UPD", "type": "STRING"},
        {"name": "LSA_LINK", "type": "STRING"},
        {"name": "LSA_LINK_NAME", "type": "STRING"},
        {"name": "MIDDLE_NAME", "type": "STRING"},
        {"name": "NAME", "type": "STRING"},
        {"name": "NEW_SEGMENT_ID", "type": "INTEGER"},
        {"name": "NGY_TO_LEAD", "type": "TIMESTAMP"},
        {"name": "NOTES", "type": "STRING"},
        {"name": "N__GI_RATING_ID", "type": "INTEGER"},
        {"name": "ONBOARDING_DATE", "type": "TIMESTAMP"},
        {"name": "ORIGINATOR_ID", "type": "STRING"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "PARTNER_RECORD_REFERENCE_ID", "type": "INTEGER"},
        {"name": "PERMISSION_CHECK_PRINT", "type": "STRING"},
        {"name": "PHONE", "type": "STRING"},
        {"name": "RELEASE_REASON_ID", "type": "INTEGER"},
        {"name": "SALES_REP_JOB_TITLE_ID", "type": "INTEGER"},
        {"name": "SALUTATION", "type": "STRING"},
        {"name": "SMS_FOR_EXP_DATE", "type": "TIMESTAMP"},
        {"name": "SMS_PROMO_ID", "type": "STRING"},
        {"name": "SMS_SENT_DATE", "type": "TIMESTAMP"},
        {"name": "SOURCES_ID", "type": "INTEGER"},
        {"name": "SPECIAL_DISCOUNT", "type": "INTEGER"},
        {"name": "STATE", "type": "STRING"},
        {"name": "SUBSIDIARY", "type": "INTEGER"},
        {"name": "TAX_CONTACT_FIRST_NAME", "type": "STRING"},
        {"name": "TAX_CONTACT_ID", "type": "INTEGER"},
        {"name": "TAX_CONTACT_LAST_NAME", "type": "STRING"},
        {"name": "TAX_CONTACT_MIDDLE_NAME", "type": "STRING"},
        {"name": "UNSUBSCRIBED", "type": "STRING"},
        {"name": "UPDATE_TIMESTAMP", "type": "TIMESTAMP"},
        {"name": "VAT_REGISTRATION_NO", "type": "STRING"},
        {"name": "VN_CODE", "type": "STRING"},
        {"name": "ZIPCODE", "type": "STRING"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            ACCOUNT_OWNER_NAME,
            ADDRESS_ONE,
            ADDRESS_THREE,
            ADDRESS_TWO,
            APPROVE_PERSON,
            BANKS_BRANCH,
            BANK_ACCOUNT,
            BANK_NAME,
            BLOCK_SMS,
            BRANCH,
            CITY,
            COUNTRY,
            CREATE_DATE,
            DATE_LAST_MODIFIED,
            DATE_OF_BIRTH,
            DATE_UPDATED,
            DEFAULT_AP_ACCOUNT_ID,
            DELIVERY_PERSON,
            DIC,
            EMAIL,
            EMPLOYEE_RECORD_REFERENCE_ID,
            ENTITY_CODE,
            ENTITY_EXTID,
            ENTITY_ID,
            ENTITY_TYPE,
            EXPIRATION_DATE_FOR_LOYALTY_S,
            FIRST_NAME,
            FROM_CAMPAIGN,
            FROM_LANDING,
            FULL_NAME,
            GLOBAL_SUBSCRIPTION_STATUS,
            ICO,
            IDENTITY_NO_,
            IDENTITY_PROVIDED_AGENCY,
            IDENTITY_PROVIDED_DATE,
            ISRELEASEDCOACHING,
            IS_INACTIVE,
            IS_LEAD__BN_BUN,
            IS_ONLINE_BILL_PAY,
            IS_UNAVAILABLE,
            LAST_MODIFIED_DATE,
            LAST_NAME,
            LAST_SALES_ACTIVITY,
            LOGIN_ACCESS,
            LOYALTY_ELIGIBILITY,
            LOYALTY_GROUP_ID,
            LOYALTY_GROUP_LEVEL,
            LOYALTY_REMAINING_POINT_VALUE,
            LOYALTY_SMS_VALUE_PENDING_UPD,
            LSA_LINK,
            LSA_LINK_NAME,
            MIDDLE_NAME,
            NAME,
            NEW_SEGMENT_ID,
            NGY_TO_LEAD,
            NOTES,
            N__GI_RATING_ID,
            ONBOARDING_DATE,
            ORIGINATOR_ID,
            PARENT_ID,
            PARTNER_RECORD_REFERENCE_ID,
            PERMISSION_CHECK_PRINT,
            PHONE,
            RELEASE_REASON_ID,
            SALES_REP_JOB_TITLE_ID,
            SALUTATION,
            SMS_FOR_EXP_DATE,
            SMS_PROMO_ID,
            SMS_SENT_DATE,
            SOURCES_ID,
            SPECIAL_DISCOUNT,
            STATE,
            SUBSIDIARY,
            TAX_CONTACT_FIRST_NAME,
            TAX_CONTACT_ID,
            TAX_CONTACT_LAST_NAME,
            TAX_CONTACT_MIDDLE_NAME,
            UNSUBSCRIBED,
            UPDATE_TIMESTAMP,
            VAT_REGISTRATION_NO,
            VN_CODE,
            ZIPCODE
        FROM
            "Vua Nem Joint Stock Company".Administrator.ENTITY
        WHERE
            DATE_LAST_MODIFIED >= '{tr[0]}'
            AND DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["ENTITY_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)