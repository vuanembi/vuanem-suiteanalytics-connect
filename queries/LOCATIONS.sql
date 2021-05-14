SELECT
    *,
    CASE
        WHEN OPENNING_DAY IS NULL THEN FIRST_TRAFFIC_DATE
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
                    STORE_MODEL.LIST_ITEM_NAME AS STORE_MODEL,
                    CASE
                        WHEN LOCATIONS.CLOSE_DATE IS NULL
                        OR LOCATIONS.CLOSE_DATE > CURRENT_DATE() THEN '0'
                        ELSE '1'
                    END AS 'ISCLOSE'
                FROM
                    "Vua Nem Joint Stock Company".Administrator.LOCATIONS LOCATIONS
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
