from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class RATING(NetSuite):
    keys = {
        "p_key": ["LIST_ID"],
        "rank_key": ["LIST_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    query = """
        SELECT
            DATE_CREATED,
            IS_RECORD_INACTIVE,
            LAST_MODIFIED_DATE,
            LIST_ID,
            LIST_ITEM_NAME,
            RATING_EXTID
        FROM
            "Vua Nem Joint Stock Company".Administrator.RATING
        WHERE
            RATING.LAST_MODIFIED_DATE >= '{{ start }}'
            AND RATING.LAST_MODIFIED_DATE <= '{{ end }}'
    """

    schema = [
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_RECORD_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LIST_ID", "type": "INTEGER"},
        {"name": "LIST_ITEM_NAME", "type": "STRING"},
        {"name": "RATING_EXTID", "type": "STRING"},
    ]
    columns = []
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
