from sqlalchemy import Column, Integer, String, DateTime

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
            STORE_TRAFFIC.DATE_0,
            STORE_TRAFFIC.DATE_CREATED,
            STORE_TRAFFIC.GENDER_ID,
            STORE_TRAFFIC.IS_INACTIVE,
            STORE_TRAFFIC.LAST_MODIFIED_DATE,
            STORE_TRAFFIC.LOCATION_ID,
            STORE_TRAFFIC.STORE_TRAFFIC_ID,
            STORE_TRAFFIC.SUBSIDIARY_ID,
            STORE_TRAFFIC.TOTAL_TIMES_OF_VISITING,
            STORE_TRAFFIC.TOTAL_VISITOR
        FROM
            "Vua Nem Joint Stock Company".Administrator.STORE_TRAFFIC STORE_TRAFFIC
        WHERE
            STORE_TRAFFIC.LAST_MODIFIED_DATE >= '{{ start }}'
            AND STORE_TRAFFIC.LAST_MODIFIED_DATE <= '{{ end }}'
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
    ]
    columns = [
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
    ]
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
