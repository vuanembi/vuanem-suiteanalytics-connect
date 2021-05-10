from pipelines import NetSuiteIncrementalJob


class STORE_TRAFFIC(NetSuiteIncrementalJob):
    p_key = ["STORE_TRAFFIC_ID"]
    incremental_key = "LAST_MODIFIED_DATE"
    partition_key = ["DATE_0"]

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
        "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".STORE_TRAFFIC STORE_TRAFFIC
    WHERE STORE_TRAFFIC.LAST_MODIFIED_DATE >= ?
    """
    schema = [
        {"name": "DATE_0", "type": "TIMESTAMP"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "GENDER_ID", "type": "STRING"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STORE_TRAFFIC_ID", "type": "INTEGER"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "TOTAL_TIMES_OF_VISITING", "type": "INTEGER"},
        {"name": "TOTAL_VISITOR", "type": "INTEGER"},
    ]
