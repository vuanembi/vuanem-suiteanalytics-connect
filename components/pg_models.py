from sqlalchemy import (
    MetaData,
    Table,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    BigInteger,
)

metadata_obj = MetaData(schema="NetSuite")

Accounts = Table(
    "ACCOUNTS",
    metadata_obj,
    Column("ACCOUNT_ID", Integer, primary_key=True),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("FULL_DESCRIPTION", String),
    Column("DESCRIPTION", String),
    Column("FULL_NAME", String),
    Column("HEADER_0", String),
    Column("SUBHEADER", String),
    Column("LOCATION_ID", Integer),
    Column("NAME", String),
    Column("ACCOUNTNUMBER", String),
    Column("TYPE_NAME", String),
)

Budget = Table(
    "BUDGET",
    metadata_obj,
    Column("BUDGET_ID", Integer, primary_key=True),
    Column("LOCATION_ID", Integer),
    Column("STARTING", DateTime(timezone=True)),
    Column("PERIODS_NAME", String),
    Column("CATEGORY_NAME", String),
    Column("AMOUNT", BigInteger),
    Column("BUDGET_ISINACTIVE", String),
)

Classes = Table(
    "CLASSES",
    metadata_obj,
    Column("CLASS_ID", Integer, primary_key=True),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("FULL_NAME", String),
    Column("ISINACTIVE", String),
    Column("CLASS_DESCRIPTION", String),
    Column("NAME", String),
    Column("PRODUCT_GROUP_CODE", String),
)

Customers = Table(
    "CUSTOMERS",
    metadata_obj,
    Column("CUSTOMER_ID", Integer, primary_key=True),
    Column("PHONE", String),
    Column("EMAIL", String),
    Column("CATEGORY_0", String),
    Column("FULL_NAME", String),
    Column("FIRSTNAME", String),
    Column("MIDDLENAME", String),
    Column("LASTNAME", String),
    Column("NAME", String),
    Column("BILLADDRESS", String),
    Column("SHIPADDRESS", String),
    Column("CITY", String),
    Column("COMPANYNAME", String),
    Column("CONVERTED_TO_ID", Integer),
    Column("COUNTRY", String),
    Column("CREATE_DATE", DateTime(timezone=True)),
    Column("DATE_CLOSED", DateTime(timezone=True)),
    Column("DATE_CONVSERSION", DateTime(timezone=True)),
    Column("DATE_FIRST_ORDER", DateTime(timezone=True)),
    Column("DATE_FIRST_SALE", DateTime(timezone=True)),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("DATE_LAST_ORDER", DateTime(timezone=True)),
    Column("DATE_LAST_SALE", DateTime(timezone=True)),
    Column("DATE_LEAD", DateTime(timezone=True)),
    Column("DATE_PROSPECT", DateTime(timezone=True)),
    Column("FROM_CAMPAIGN", String),
    Column("FROM_LANDING", String),
    Column("HOME_PHONE", String),
    Column("ISINACTIVE", String),
    Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
    Column("LEAD_SOURCE_ID", Integer),
    Column("SMS_PROMO_ID", String),
    Column("SOURCES_ID", Integer),
    Column("STATUS", String),
    Column("SUBSIDIARY_ID", Integer),
)

DeliveryPerson = Table(
    "DELIVERY_PERSON",
    metadata_obj,
    Column("DATE_CREATED", DateTime(timezone=True)),
    Column("DELIVERY_PERSON_EXTID", String),
    Column("DELIVERY_PERSON_ID", Integer, primary_key=True),
    Column("DELIVERY_PERSON_NAME", String),
    Column("IS_INACTIVE", String),
    Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
    Column("REF__EMPLOYEE_ID", Integer),
    Column("VN_CODE", String),
)

Departments = Table(
    "DEPARTMENTS",
    metadata_obj,
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("DEPARTMENT_DESCRIPTION", String),
    Column("DEPARTMENT_EXTID", String),
    Column("DEPARTMENT_ID", Integer, primary_key=True),
    Column("FULL_NAME", String),
    Column("ISINACTIVE", String),
    Column("IS_INCLUDING_CHILD_SUBS", String),
    Column("NAME", String),
    Column("PARENT_ID", Integer),
)

Employees = Table(
    "EMPLOYEES",
    metadata_obj,
    Column("EMPLOYEE_ID", Integer, primary_key=True),
    Column("VN_CODE", String),
    Column("FULL_NAME", String),
    Column("FIRSTNAME", String),
    Column("MIDDLENAME", String),
    Column("LASTNAME", String),
    Column("NAME", String),
    Column("JOBTITLE", String),
    Column("PHONE", String),
    Column("MOBILE_PHONE", String),
    Column("EMAIL", String),
    Column("GENDER", String),
    Column("BIRTHDATE", DateTime(timezone=True)),
    Column("COMMENTS", String),
    Column("CREATE_DATE", DateTime(timezone=True)),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
    Column("HIREDDATE", DateTime(timezone=True)),
    Column("ONBOARDING_DATE", DateTime(timezone=True)),
    Column("RELEASEDATE", DateTime(timezone=True)),
    Column("RELEASE_REASON_ID", Integer),
    Column("DELIVERY_PERSON", String),
    Column("DEPARTMENT_ID", Integer),
    Column("LOCATION_ID", Integer),
    Column("EMPLOYEE_TYPE_ID", Integer),
    Column("IDENTITY_NO_", String),
    Column("IDENTITY_PROVIDED_AGENCY", String),
    Column("IDENTITY_PROVIDED_DATE", DateTime(timezone=True)),
    Column("ISINACTIVE", String),
    Column("ISSALESREP", String),
    Column("JOB_DESCRIPTION", String),
    Column("STATUS", String),
    Column("SUBSIDIARY_ID", Integer),
    Column("SUPERVISOR_ID", Integer),
)

Items = Table(
    "ITEMS",
    metadata_obj,
    Column("ITEM_ID", Integer, primary_key=True),
    Column("CLASS_ID", Integer),
    Column("FULL_NAME", String),
    Column("DISPLAYNAME", String),
    Column("NEW_ITEM_CODE", String),
    Column("OLD_VN_CODE", String),
    Column("VN__OLD_ITEM_CODE", String),
    Column("PRODUCT_CODE", String),
    Column("TYPE_NAME", String),
    Column("SALESPRICE", String),
    Column("SALESDESCRIPTION", String),
    Column("STATUS", String),
    Column("COLOR", String),
    Column("FEATURE", String),
    Column("MATERIAL", String),
    Column("SEGMENT", String),
    Column("MATTRESS_COMFORT_LEVEL", String),
    Column("PROMOTION_APPLIED_TO", String),
    Column("THICKNESS", Integer),
    Column("THREADCOUNT", String),
    Column("VENDOR_ID", Integer),
    Column("WARRANTY", Integer),
    Column("WEIGHT", Integer),
    Column("KCH_THC_NG_GI", String),
    Column("LENGTH_0", Integer),
    Column("WIDTH", Integer),
    Column("PROMOTION_START_DATE", DateTime(timezone=True)),
    Column("PROMOTION_END_DATE", DateTime(timezone=True)),
    Column("BUY_X_AMOUNT", Integer),
    Column("BUY_X_ITEM_ID", Integer),
    Column("BUY_X_QUANTITY", Integer),
    Column("CREATED", DateTime(timezone=True)),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("EXPENSE_ACCOUNT_ID", Integer),
    Column("ISINACTIVE", String),
    Column("ISONLINE", String),
    Column("ITEM_EXTID", String),
    Column("CODE_OF_SUPPLY_ID", Integer),
    Column("LOYALTY_CATEGORY_ID", Integer),
)

Locations = Table(
    "LOCATIONS",
    metadata_obj,
    Column("LOCATION_ID", Integer, primary_key=True),
    Column("STORE_NAME", String),
    Column("ISINACTIVE", String),
    Column("SUBSIDIARY_ID", Integer),
    Column("OPENNING_DAY", DateTime(timezone=True)),
    Column("CLOSE_DATE", DateTime(timezone=True)),
    Column("CURRENT_ASM", String),
    Column("ASM_ID", Integer),
    Column("ISCLOSE", Integer),
    Column("EMAIL", String),
    Column("FIRST_TRAFFIC_DATE", DateTime(timezone=True)),
    Column("CITY_ID", String),
    Column("AREA_M2", Float),
    Column("FRONT_LENGTH", Float),
    Column("STORE_MODEL", String),
    Column("OPENNING_DAY2", DateTime(timezone=True)),
)

SystemNotesPrice = Table(
    "SYSTEM_NOTES_PRICE",
    metadata_obj,
    Column("DATE_CREATED", DateTime(timezone=True)),
    Column("ITEM_ID", Integer),
    Column("VALUE_NEW", String),
    Column("OPERATION", String),
)

Vendors = Table(
    "VENDORS",
    metadata_obj,
    Column("VENDOR_ID", Integer, primary_key=True),
    Column("NAME", String),
    Column("FULL_NAME", String),
    Column("VENDOR_TYPE", String),
)

NS2PromotionCode = Table(
    "ns2_promotionCode",
    metadata_obj,
    Column("enddate", DateTime(timezone=True)),
    Column("startdate", DateTime(timezone=True)),
    Column("discount", Integer),
    Column("description", String),
    Column("discounttype", String),
    Column("discounteditemssavedsearch", Integer),
    Column("displaylinediscounts", String),
    Column("externalid", String),
    Column("fixedprice", Integer),
    Column("id", Integer, primary_key=True),
    Column("isinactive", String),
    Column("itemquantifier", Integer),
    Column("location", String),
    Column("name", String),
    Column("promotiontype", String),
    Column("rate", Float),
    Column("repeatdiscount", String),
    Column("lastmodifieddate", DateTime(timezone=True)),
)

ItemLocationMap = Table(
    "ITEM_LOCATION_MAP",
    metadata_obj,
    Column("NEW_ITEM_CODE", String),
    Column("ITEM_ID", Integer, primary_key=True),
    Column("LOCATION_ID", Integer, primary_key=True),
    Column("DISPLAYNAME", String),
    Column("ON_HAND_COUNT", Integer),
)


# ----------------------------

Cases = Table(
    "CASES",
    metadata_obj,
    Column("ASSIGNED_ID", Integer),
    Column("CASE_ID", Integer, primary_key=True),
    Column("CASE_NUMBER", Integer),
    Column("CAI_THIEN", String),
    Column("CREATE_DATE", DateTime(timezone=True)),
    Column("LY_DO_CHO_SO_DIEM", String),
    Column("LY_DO_KHONG_MUA_HANG", Integer),
    Column("NAME", String),
    Column("NV_BN_HNG_ID", Integer),
    Column("BAOHANH_ID", Integer),
    Column("NVGH_ID", Integer),
    Column("HOTRO_ID", Integer),
    Column("RATING_KHCH_VO_CH_CHA_MUA__ID", Integer),
    Column("RATING__KHCH_DIGITALHOTLINE_ID", Integer),
    Column("RATING__SN_PHM_A_DNG_PHON_ID", Integer),
    Column("RATING__SAU_KHI_GIAO_HNG_ID", Integer),
    Column("RATING__SAU_KHI_LN_SO_ID", Integer),
    Column("SAO_CHO_NVBH_ID", Integer),
    Column("SAO_CHO_NVGH_ID", Integer),
    Column("SAO_CHO_NV_BO_HNH_M_ID", Integer),
    Column("SAO_CHO_NV_H_TR_ID", Integer),
    Column("SO_LOCATION_ID", Integer),
    Column("SO_REFERENCE_ID", Integer),
    Column("NGY_HON_THNH_KHIU_NIBO_", DateTime(timezone=True)),
    Column("CASE_TYPE_ID", Integer),
    Column("NPS_SCORE_ID", Integer),
    Column("CASE_ORIGIN_ID", Integer),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
)

DeletedRecords = Table(
    "DELETED_RECORDS",
    metadata_obj,
    Column("CUSTOM_RECORD_TYPE", String),
    Column("DATE_DELETED", DateTime(timezone=True)),
    Column("ENTITY_ID", Integer),
    Column("ENTITY_NAME", String),
    Column("IS_CUSTOM_LIST", String),
    Column("NAME", String),
    Column("RECORD_BASE_TYPE", String),
    Column("RECORD_ID", Integer),
    Column("RECORD_TYPE_NAME", String),
)

LoyaltyTransaction = Table(
    "LOYALTY_TRANSACTION",
    metadata_obj,
    Column("LOYALTY_TRANSACTION_ID", Integer, primary_key=True),
    Column("AMOUNT", Integer),
    Column("CUSTOMER_ID", Integer),
    Column("DATE_CREATED", DateTime(timezone=True)),
    Column("DOCUMENT_NO", String),
    Column("EXPIRED_DATE", DateTime(timezone=True)),
    Column("IS_INACTIVE", String),
    Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
    Column("LOYALTY_CUSTOMER_GROUP_ID", Integer),
    Column("LOYALTY_LOCATION_GROUP_ID", Integer),
    Column("LOYALTY_PROGRAM_ID", Integer),
    Column("LOYALTY_TRANSACTION_EXTID", String),
    Column("POINT_0", Integer),
    Column("REWARD_RATE", Integer),
    Column("TRANSACTION_DATE", DateTime(timezone=True)),
    Column("TRANSACTION_TYPE", String),
    Column("TRANS_ID", Integer),
    Column("TRANS_LOCATION_ID", Integer),
    Column("UPDATE_TIME_", DateTime(timezone=True)),
    Column("VALID_FROM_DATE", DateTime(timezone=True)),
    Column("VALID_TO_DATE", DateTime(timezone=True)),
)

ServiceAddonSOMap = Table(
    "SERVICE_ADDON_SO_MAP",
    metadata_obj,
    Column("LIST_SERVICE_ADD_ON_SO_ID", Integer),
    Column("TRANSACTION_ID", Integer, primary_key=True),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
)

ServiceAddonTOMap = Table(
    "SERVICE_ADDON_TO_MAP",
    metadata_obj,
    Column("LIST_SERVICE_ADD_ON_TO_ID", Integer),
    Column("TRANSACTION_ID", Integer, primary_key=True),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
)

StoreTraffic = Table(
    "STORE_TRAFFIC",
    metadata_obj,
    Column("DATE_0", DateTime(timezone=True)),
    Column("DATE_CREATED", DateTime(timezone=True)),
    Column("GENDER_ID", Integer),
    Column("IS_INACTIVE", String),
    Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
    Column("LOCATION_ID", Integer),
    Column("STORE_TRAFFIC_ID", Integer, primary_key=True),
    Column("SUBSIDIARY_ID", Integer),
    Column("TOTAL_TIMES_OF_VISITING", Integer),
    Column("TOTAL_VISITOR", Integer),
)

SupportPersonMap = Table(
    "SUPPORT_PERSON_MAP",
    metadata_obj,
    Column("DELIVERY_PERSON_ID", Integer, primary_key=True),
    Column("TRANSACTION_ID", Integer, primary_key=True),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
)

Transactions = Table(
    "TRANSACTIONS",
    metadata_obj,
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
)

TransactionLines = Table(
    "TRANSACTION_LINES",
    metadata_obj,
    Column("TRANSACTION_ID", Integer, primary_key=True),
    Column("TRANSACTION_LINE_ID", Integer, primary_key=True),
    Column("ACCOUNT_ID", Integer),
    Column("AMOUNT", Integer),
    Column("AMOUNT_BEFORE_DISCOUNT", Integer),
    Column("AMOUNT_FOREIGN_LINKED", Integer),
    Column("CLASS_ID", Integer),
    Column("COMPANY_ID", Integer),
    Column("DATE_CLOSED", DateTime(timezone=True)),
    Column("DATE_CREATED", DateTime(timezone=True)),
    Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
    Column("DELIVERY_METHOD_ID", Integer),
    Column("DELIVER_LOCATION_ID", Integer),
    Column("DEPARTMENT_ID", Integer),
    Column("EXPECTED_DELIVERY_DATE_SO", DateTime(timezone=True)),
    Column("GROSS_AMOUNT", Integer),
    Column("IS_COST_LINE", String),
    Column("ITEM_COUNT", Integer),
    Column("ITEM_GROUP_PROMOTION_ID", Integer),
    Column("ITEM_ID", Integer),
    Column("ITEM_TYPE", String),
    Column("ITEM_UNIT_PRICE", String),
    Column("LOCATION_ID", Integer),
    Column("MEMO", String),
    Column("NET_AMOUNT", Integer),
    Column("QUANTITY_RECEIVED_IN_SHIPMENT", Integer),
    Column("SUBSIDIARY_ID", Integer),
    Column("TRANSACTION_ORDER", Integer),
    Column("TRANSFER_ORDER_ITEM_LINE", Integer),
    Column("TRANSFER_ORDER_LINE_TYPE", String),
    Column("VENDOR_ID", Integer),
    Column("TRANSACTIONS_DATE_LAST_MODIFIED", DateTime(timezone=True)),
)

NS2TranPromotion = Table(
    "ns2_tranPromotion",
    metadata_obj,
    Column("transaction", Integer),
    Column("couponcode", Integer),
    Column("eligiblefreegifts", Float),
    Column("freegiftsadded", Float),
    Column("promocode", Integer),
    Column("promotiontype", String),
    Column("purchasediscount", Float),
    Column("shippingdiscount", Float),
    Column("lastmodifieddate", DateTime(timezone=True)),
)

NS2TransactionLine = Table(
    "ns2_transactionLine",
    metadata_obj,
    Column("TRANSACTION_ID", Integer),
    Column("ACCOUNT_ID", Integer),
    Column("netamount", Float),
    Column("rate", Float),
    Column("rateamount", Float),
    Column("linelastmodifieddate", DateTime(timezone=True)),
    Column("ratepercent", Float),
)

NS2CouponCode = Table(
    "ns2_couponCode",
    metadata_obj,
    Column("code", String),
    Column("datesent", DateTime(timezone=True)),
    Column("externalid", String),
    Column("id", Integer, primary_key=True),
    Column("promotion", Integer),
)
