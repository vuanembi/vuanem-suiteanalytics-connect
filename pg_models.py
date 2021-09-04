import copy
from abc import ABCMeta, abstractmethod

from sqlalchemy import MetaData, Table, Column, Integer, String, DateTime, Float, BigInteger
from sqlalchemy.orm import declarative_base

Base = declarative_base()
metadata_obj = MetaData()


class Accounts:
    table = "ACCOUNTS"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Budget:
    table = "BUDGET"
    columns = [
        Column("BUDGET_ID", Integer, primary_key=True),
        Column("LOCATION_ID", Integer),
        Column("STARTING", DateTime(timezone=True)),
        Column("PERIODS_NAME", String),
        Column("CATEGORY_NAME", String),
        Column("AMOUNT", BigInteger),
        Column("BUDGET_ISINACTIVE", String),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Classes:
    table = "CLASSES"
    columns = [
        Column("CLASS_ID", Integer, primary_key=True),
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("FULL_NAME", String),
        Column("ISINACTIVE", String),
        Column("CLASS_DESCRIPTION", String),
        Column("NAME", String),
        Column("PRODUCT_GROUP_CODE", String),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Customers:
    table = "CUSTOMERS"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class DeliveryPerson:
    table = "DELIVERY_PERSON"
    columns = [
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("DELIVERY_PERSON_EXTID", String),
        Column("DELIVERY_PERSON_ID", Integer, primary_key=True),
        Column("DELIVERY_PERSON_NAME", String),
        Column("IS_INACTIVE", String),
        Column("LAST_MODIFIED_DATE", DateTime(timezone=True)),
        Column("REF__EMPLOYEE_ID", Integer),
        Column("VN_CODE", String),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Departments:
    table = "DEPARTMENTS"
    columns = [
        Column("DATE_LAST_MODIFIED", DateTime(timezone=True)),
        Column("DEPARTMENT_DESCRIPTION", String),
        Column("DEPARTMENT_EXTID", String),
        Column("DEPARTMENT_ID", Integer, primary_key=True),
        Column("FULL_NAME", String),
        Column("ISINACTIVE", String),
        Column("IS_INCLUDING_CHILD_SUBS", String),
        Column("NAME", String),
        Column("PARENT_ID", Integer),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Employees:
    table = "EMPLOYEES"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Items:
    table = "ITEMS"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Locations:
    table = "LOCATIONS"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class SystemNotesPrice:
    table = "SYSTEM_NOTES_PRICE"
    columns = [
        Column("DATE_CREATED", DateTime(timezone=True)),
        Column("ITEM_ID", Integer),
        Column("VALUE_NEW", String),
        Column("OPERATION", String),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class Vendors:
    table = "VENDORS"
    columns = [
        Column("VENDOR_ID", Integer, primary_key=True),
        Column("NAME", String),
        Column("FULL_NAME", String),
        Column("VENDOR_TYPE", String),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class NS2_PromotionCode:
    table = "ns2_promotionCode"
    columns = [
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
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))


class ItemLocationMap:
    table = "ITEM_LOCATION_MAP"
    columns = [
        Column("NEW_ITEM_CODE", String),
        Column("ITEM_ID", Integer, primary_key=True),
        Column("LOCATION_ID", Integer, primary_key=True),
        Column("DISPLAYNAME", String),
        Column("ON_HAND_COUNT", Integer),
    ]
    main = Table(table, metadata_obj, *copy.deepcopy(columns))
    stage = Table(f"_stage_{table}", metadata_obj, *copy.deepcopy(columns))
