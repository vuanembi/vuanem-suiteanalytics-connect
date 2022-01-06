from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class CITYPROVINCE_LIST(NetSuite):
    keys = {
        "p_key": ["CITYPROVINCE_LIST_ID"],
        "rank_key": ["CITYPROVINCE_LIST_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            CITYPROVINCE_LIST_EXTID,
            CITYPROVINCE_LIST_ID,
            CITYPROVINCE_LIST_NAME,
            DATE_CREATED,
            IS_INACTIVE,
            LAST_MODIFIED_DATE,
            PARENT_ID
        FROM
            "Vua Nem Joint Stock Company".Administrator.CITYPROVINCE_LIST
        WHERE
            LAST_MODIFIED_DATE >= '{{ start }}'
            AND LAST_MODIFIED_DATE <= '{{ end }}'
    """
    schema = [
        {"name": "CITYPROVINCE_LIST_EXTID", "type": "STRING"},
        {"name": "CITYPROVINCE_LIST_ID", "type": "INTEGER"},
        {"name": "CITYPROVINCE_LIST_NAME", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "PARENT_ID", "type": "INTEGER"},
    ]
    columns = []
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
