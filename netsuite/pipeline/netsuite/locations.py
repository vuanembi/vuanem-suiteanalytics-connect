from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "LOCATIONS",
    [
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STORE_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "OPENNING_DAY", "type": "TIMESTAMP"},
        {"name": "CLOSE_DATE", "type": "TIMESTAMP"},
        {"name": "CURRENT_ASM", "type": "STRING"},
        {"name": "ASM_ID", "type": "INTEGER"},
        {"name": "ISCLOSE", "type": "INTEGER"},
        {"name": "EMAIL", "type": "STRING"},
        {"name": "FIRST_TRAFFIC_DATE", "type": "TIMESTAMP"},
        {"name": "CITY_ID", "type": "STRING"},
        {"name": "AREA_M2", "type": "FLOAT"},
        {"name": "FRONT_LENGTH", "type": "FLOAT"},
        {"name": "STORE_MODEL", "type": "STRING"},
        {"name": "OPENNING_DAY2", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            *,
            CASE
                WHEN OPENNING_DAY IS NULL THEN FIRST_TRAFFIC_DATE
                ELSE OPENNING_DAY
            END AS OPENNING_DAY2
        FROM
            (
                SELECT
                    LOCATION.LOCATION_ID,
                    LEFT(LOCATION.STORE_NAME, 3) AS CITY_ID,
                    LOCATION.STORE_NAME,
                    LOCATION.ISINACTIVE,
                    LOCATION.SUBSIDIARY_ID,
                    LOCATION.OPENNING_DAY,
                    LOCATION.CLOSE_DATE,
                    LOCATION.CURRENT_ASM,
                    LOCATION.ASM_ID,
                    LOCATION.AREA_M2,
                    LOCATION.FRONT_LENGTH,
                    LOCATION.STORE_MODEL,
                    LOCATION.ISCLOSE,
                    LOCATION.EMAIL,
                    FIRST_TRAFFIC_DATE.FIRST_TRAFFIC_DATE
                FROM
                    (
                        SELECT
                            LOCATIONS.LOCATION_ID,
                            REPLACE(
                                REPLACE(LOCATIONS.NAME, 'HCM', 'HMI'),
                                'VLG',
                                'VLO'
                            ) AS STORE_NAME,
                            LOCATIONS.ISINACTIVE,
                            SUBSIDIARIES.SUBSIDIARY_ID,
                            LOCATIONS.OPENNING_DAY,
                            LOCATIONS.CLOSE_DATE,
                            EMPLOYEES.FULL_NAME AS 'CURRENT_ASM',
                            LOCATIONS.ASM_ID,
                            LOCATIONS.AREA_M2,
                            LOCATIONS.FRONT_LENGTH,
                            LOCATIONS.EMAIL,
                            STORE_MODEL.LIST_ITEM_NAME AS STORE_MODEL,
                            CASE
                                WHEN LOCATIONS.CLOSE_DATE IS NULL
                                OR LOCATIONS.CLOSE_DATE > CURRENT_DATE() THEN '0'
                                ELSE '1'
                            END AS 'ISCLOSE'
                        FROM
                            "Vua Nem Joint Stock Company".Administrator.LOCATIONS
                            INNER JOIN "Vua Nem Joint Stock Company".Administrator.SUBSIDIARY_LOCATION_MAP SUBSIDIARY_LOCATION_MAP ON SUBSIDIARY_LOCATION_MAP.LOCATION_ID = LOCATIONS.LOCATION_ID
                            INNER JOIN "Vua Nem Joint Stock Company".Administrator.SUBSIDIARIES SUBSIDIARIES ON SUBSIDIARY_LOCATION_MAP.SUBSIDIARY_ID = SUBSIDIARIES.SUBSIDIARY_ID
                            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.EMPLOYEES EMPLOYEES ON LOCATIONS.ASM_ID = EMPLOYEES.EMPLOYEE_ID
                            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.STORE_MODEL STORE_MODEL ON STORE_MODEL.LIST_ID = LOCATIONS.STORE_MODEL_ID
                    ) AS LOCATION
                    LEFT JOIN (
                        SELECT
                            REPLACE(LOCATIONS.NAME, 'HCM', 'HMI') AS STORE_NAME,
                            MIN(STORE_TRAFFIC.DATE_0) AS FIRST_TRAFFIC_DATE
                        FROM
                            "Vua Nem Joint Stock Company".Administrator.LOCATIONS
                            LEFT JOIN "Vua Nem Joint Stock Company".Administrator.STORE_TRAFFIC ON STORE_TRAFFIC.LOCATION_ID = LOCATIONS.LOCATION_ID
                        WHERE
                            LOCATIONS.NAME IS NOT NULL
                        GROUP BY
                            REPLACE(LOCATIONS.NAME, 'HCM', 'HMI')
                    ) AS FIRST_TRAFFIC_DATE ON FIRST_TRAFFIC_DATE.STORE_NAME = LOCATION.STORE_NAME
            )
    """,
)
