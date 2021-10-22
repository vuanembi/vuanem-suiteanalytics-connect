from sqlalchemy import Column, Integer, String, DateTime

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ITEMS(NetSuite):
    query = """
        SELECT
            ITEMS.ITEM_ID,
            ITEMS.CLASS_ID,
            ITEMS.FULL_NAME,
            ITEMS.DISPLAYNAME,
            ITEMS.NEW_ITEM_CODE,
            ITEMS.OLD_VN_CODE,
            ITEMS.VN__OLD_ITEM_CODE,
            ITEMS.PRODUCT_CODE,
            ITEMS.TYPE_NAME,
            ITEMS.SALESPRICE,
            ITEMS.SALESDESCRIPTION,
            INVENTORY_STATUS.LIST_ITEM_NAME AS 'STATUS',
            LIST__ITEM_COLOR.LIST_ITEM_NAME AS 'COLOR',
            LIST__ITEM_FEATURE.LIST_ITEM_NAME AS 'FEATURE',
            LIST__ITEM_MATERIAL.LIST_ITEM_NAME AS 'MATERIAL',
            SEGMENT_LIST.LIST_ITEM_NAME AS 'SEGMENT',
            LIST__MATTRESS_COMFORT_LEVELS.LIST_ITEM_NAME AS 'MATTRESS_COMFORT_LEVEL',
            PROMOTION_APPLIED_TO_VALUE.LIST_ITEM_NAME AS 'PROMOTION_APPLIED_TO',
            ITEMS.THICKNESS,
            ITEMS.THREADCOUNT,
            ITEMS.VENDOR_ID,
            ITEMS.WARRANTY,
            ITEMS.WEIGHT,
            ITEMS.KCH_THC_NG_GI,
            ITEMS.LENGTH_0,
            ITEMS.WIDTH,
            ITEMS.PROMOTION_START_DATE,
            ITEMS.PROMOTION_END_DATE,
            ITEMS.BUY_X_AMOUNT,
            ITEMS.BUY_X_ITEM_ID,
            ITEMS.BUY_X_QUANTITY,
            ITEMS.CREATED,
            ITEMS.DATE_LAST_MODIFIED,
            ITEMS.EXPENSE_ACCOUNT_ID,
            ITEMS.ISINACTIVE,
            ITEMS.ISONLINE,
            ITEMS.ITEM_EXTID,
            ITEMS.CODE_OF_SUPPLY_ID,
            ITEMS.LOYALTY_CATEGORY_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.ITEMS
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.LIST__ITEM_COLOR ON LIST__ITEM_COLOR.LIST_ID = ITEMS.COLOR_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.LIST__ITEM_FEATURE ON LIST__ITEM_FEATURE.LIST_ID = ITEMS.FEATURE_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.INVENTORY_STATUS ON INVENTORY_STATUS.LIST_ID = ITEMS.ITEM_STATUS_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.LIST__ITEM_MATERIAL ON LIST__ITEM_MATERIAL.LIST_ID = ITEMS.MATERIAL_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.LIST__MATTRESS_COMFORT_LEVELS ON LIST__MATTRESS_COMFORT_LEVELS.LIST_ID = ITEMS.MATTRESS_COMFORT_LEVELS_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.PROMOTION_APPLIED_TO_VALUE ON PROMOTION_APPLIED_TO_VALUE.LIST_ID = ITEMS.PROMOTION_APPLIED_TO_ID
            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.SEGMENT_LIST ON SEGMENT_LIST.LIST_ID = ITEMS.SEGMENT_ID
    """
    schema = [
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "CLASS_ID", "type": "INTEGER"},
        {"name": "FULL_NAME", "type": "STRING"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "NEW_ITEM_CODE", "type": "STRING"},
        {"name": "OLD_VN_CODE", "type": "STRING"},
        {"name": "VN__OLD_ITEM_CODE", "type": "STRING"},
        {"name": "PRODUCT_CODE", "type": "STRING"},
        {"name": "TYPE_NAME", "type": "STRING"},
        {"name": "SALESPRICE", "type": "STRING"},
        {"name": "SALESDESCRIPTION", "type": "STRING"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "COLOR", "type": "STRING"},
        {"name": "FEATURE", "type": "STRING"},
        {"name": "MATERIAL", "type": "STRING"},
        {"name": "SEGMENT", "type": "STRING"},
        {"name": "MATTRESS_COMFORT_LEVEL", "type": "STRING"},
        {"name": "PROMOTION_APPLIED_TO", "type": "STRING"},
        {"name": "THICKNESS", "type": "INTEGER"},
        {"name": "THREADCOUNT", "type": "STRING"},
        {"name": "VENDOR_ID", "type": "INTEGER"},
        {"name": "WARRANTY", "type": "INTEGER"},
        {"name": "WEIGHT", "type": "INTEGER"},
        {"name": "KCH_THC_NG_GI", "type": "STRING"},
        {"name": "LENGTH_0", "type": "INTEGER"},
        {"name": "WIDTH", "type": "INTEGER"},
        {"name": "PROMOTION_START_DATE", "type": "TIMESTAMP"},
        {"name": "PROMOTION_END_DATE", "type": "TIMESTAMP"},
        {"name": "BUY_X_AMOUNT", "type": "INTEGER"},
        {"name": "BUY_X_ITEM_ID", "type": "INTEGER"},
        {"name": "BUY_X_QUANTITY", "type": "INTEGER"},
        {"name": "CREATED", "type": "TIMESTAMP"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "EXPENSE_ACCOUNT_ID", "type": "INTEGER"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "ISONLINE", "type": "STRING"},
        {"name": "ITEM_EXTID", "type": "STRING"},
        {"name": "CODE_OF_SUPPLY_ID", "type": "INTEGER"},
        {"name": "LOYALTY_CATEGORY_ID", "type": "INTEGER"},
    ]
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
    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
