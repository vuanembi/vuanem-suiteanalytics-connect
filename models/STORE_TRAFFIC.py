from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class STORE_TRAFFIC(NetSuite):
    keys = {
        "p_key": ["STORE_TRAFFIC_ID"],
        "rank_key": ["STORE_TRAFFIC_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            DATE_0,
            DATE_CREATED,
            GENDER_ID,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            LOCATION_ID,
            STORE_TRAFFIC_ID,
            SUBSIDIARY_ID,
            TOTAL_TIMES_OF_VISITING,
            TOTAL_VISITOR,
            AVERAGE_NO__OF_ITEMS_PER_SALE,
            AVERAGE_VALUE_PER_SALES_ORDER,
            CONVERTER_RATE,
            EMPLOYEE_REFERENCE_ID,
            NO__OF_SALES_ORDER,
            NO__OF_SELLING_ITEMS,
            PARENT_ID,
            STORE_TRAFFIC_EXTID,
            SUBSIDIARY_ID,
            TIME_SLOT_ID,
            TOTAL_SALES_ORDER_VALUE,
            TRAFFIC_RATE,
            TRAFFIC_SOURCES_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.STORE_TRAFFIC STORE_TRAFFIC
        WHERE
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
        {"name": "DATE_0", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "GENDER_ID", "type": "INTEGER"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STORE_TRAFFIC_ID", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TOTAL_TIMES_OF_VISITING", "type": "INTEGER"},
        {"name": "TOTAL_VISITOR", "type": "INTEGER"},
        {"name": "AVERAGE_NO__OF_ITEMS_PER_SALE", "type": "INTEGER"},
        {"name": "AVERAGE_VALUE_PER_SALES_ORDER", "type": "INTEGER"},
        {"name": "CONVERTER_RATE", "type": "INTEGER"},
        {"name": "EMPLOYEE_REFERENCE_ID", "type": "INTEGER"},
        {"name": "NO__OF_SALES_ORDER", "type": "INTEGER"},
        {"name": "NO__OF_SELLING_ITEMS", "type": "INTEGER"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "STORE_TRAFFIC_EXTID", "type": "STRING"},
        {"name": "TIME_SLOT_ID", "type": "INTEGER"},
        {"name": "TOTAL_SALES_ORDER_VALUE", "type": "INTEGER"},
        {"name": "TRAFFIC_RATE", "type": "INTEGER"},
        {"name": "TRAFFIC_SOURCES_ID", "type": "INTEGER"},
    ]
    columns = []
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
