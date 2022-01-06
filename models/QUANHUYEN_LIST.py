from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class QUANHUYEN_LIST(NetSuite):
    keys = {
        "p_key": ["QUNHUYN_LIST_ID"],
        "rank_key": ["QUNHUYN_LIST_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            CITYPROVINE_ID,
            DATE_CREATED,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            PARENT_ID,
            QUNHUYN_LIST_EXTID,
            QUNHUYN_LIST_ID,
            QUNHUYN_LIST_NAME
        FROM
            "Vua Nem Joint Stock Company".Administrator.QUẬNHUYỆN_LIST
        WHERE
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
        {"name": "CITYPROVINE_ID", "type": "INTEGER"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "PARENT_ID", "type": "INTEGER"},
        {"name": "QUNHUYN_LIST_EXTID", "type": "STRING"},
        {"name": "QUNHUYN_LIST_ID", "type": "INTEGER"},
        {"name": "QUNHUYN_LIST_NAME", "type": "STRING"},
    ]
    columns = []
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
