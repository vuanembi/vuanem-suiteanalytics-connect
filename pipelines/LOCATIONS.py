from pipelines import NetSuiteJob


class LOCATIONS(NetSuiteJob):
    query = """
    SELECT
        LOCATION.LOCATION_ID,
        LOCATION.STORE_NAME,
        LOCATION.ISINACTIVE,
        LOCATION.SUBSIDIARY_ID,
        LOCATION.OPENNING_DAY,
        LOCATION.CLOSE_DATE,
        LOCATION.CURRENT_ASM,
        LOCATION.ASM_ID,
        LOCATION.ISCLOSE,
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
                CASE
                    WHEN LOCATIONS.CLOSE_DATE IS NULL THEN '0'
                    ELSE '1'
                END AS 'ISCLOSE'
            FROM
                "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".LOCATIONS LOCATIONS
                INNER JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".SUBSIDIARY_LOCATION_MAP SUBSIDIARY_LOCATION_MAP ON SUBSIDIARY_LOCATION_MAP.LOCATION_ID = LOCATIONS.LOCATION_ID
                INNER JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".SUBSIDIARIES SUBSIDIARIES ON SUBSIDIARY_LOCATION_MAP.SUBSIDIARY_ID = SUBSIDIARIES.SUBSIDIARY_ID
                LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".EMPLOYEES EMPLOYEES ON LOCATIONS.ASM_ID = EMPLOYEES.EMPLOYEE_ID
        ) AS LOCATION
        LEFT JOIN (
            SELECT
                REPLACE(LOCATIONS.NAME, 'HCM', 'HMI') AS STORE_NAME,
                MIN(STORE_TRAFFIC.DATE_0) AS FIRST_TRAFFIC_DATE
            FROM
                "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".LOCATIONS
                LEFT JOIN "Vua Nem Joint Stock Company"."Vua Nem - Storehouse Officer".STORE_TRAFFIC ON STORE_TRAFFIC.LOCATION_ID = LOCATIONS.LOCATION_ID
            WHERE
                LOCATIONS.NAME IS NOT NULL
            GROUP BY
                REPLACE(LOCATIONS.NAME, 'HCM', 'HMI')
        ) AS FIRST_TRAFFIC_DATE ON FIRST_TRAFFIC_DATE.STORE_NAME = LOCATION.STORE_NAME
    """

    schema = [
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "STORE_NAME", "type": "STRING"},
        {"name": "ISINACTIVE", "type": "STRING"},
        {"name": "SUBSIDIARY_ID", "type": "INTEGER"},
        {"name": "OPENNING_DAY", "type": "TIMESTAMP"},
        {"name": "CLOSE_DATE", "type": "TIMESTAMP"},
        {"name": "CURRENT_ASM", "type": "STRING"},
        {"name": "ASM_ID", "type": "INTEGER"},
        {"name": "ISCLOSE", "type": "INTEGER"},
        {"name": "FIRST_TRAFFIC_DATE", "type": "TIMESTAMP"},
    ]
