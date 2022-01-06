from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ADDRESSES(NetSuite):
    keys = {
        "p_key": ["ADDRESS_ID"],
        "rank_key": ["ADDRESS_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    query = """
        SELECT
            ADDRESS,
            ADDRESS_ID,
            ADDRESS_LINE_1,
            ADDRESS_LINE_2,
            ADDRESS_LINE_3,
            ATTENTION,
            CITY,
            CITYPROVINCE_ID,
            COMPANY,
            COUNTRY,
            DATE_LAST_MODIFIED,
            DISTRICTS_ID,
            IS_OVERRIDDEN,
            PHONE,
            STATE,
            ZIP
        FROM
            "Vua Nem Joint Stock Company".Administrator.ADDRESSES
        WHERE
            DATE_LAST_MODIFIED >= '{{ start }}'
            AND DATE_LAST_MODIFIED <= '{{ end }}'
    """
    schema = [
        {"name": "ADDRESS", "type": "STRING"},
        {"name": "ADDRESS_ID", "type": "INTEGER"},
        {"name": "ADDRESS_LINE_1", "type": "STRING"},
        {"name": "ADDRESS_LINE_2", "type": "STRING"},
        {"name": "ADDRESS_LINE_3", "type": "STRING"},
        {"name": "ATTENTION", "type": "STRING"},
        {"name": "CITY", "type": "STRING"},
        {"name": "CITYPROVINCE_ID", "type": "INTEGER"},
        {"name": "COMPANY", "type": "STRING"},
        {"name": "COUNTRY", "type": "STRING"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
        {"name": "DISTRICTS_ID", "type": "INTEGER"},
        {"name": "IS_OVERRIDDEN", "type": "STRING"},
        {"name": "PHONE", "type": "STRING"},
        {"name": "STATE", "type": "STRING"},
        {"name": "ZIP", "type": "STRING"},
    ]
    columns = []
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        # loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
