from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite_connection

pipeline = Pipeline(
    "ITEM_LOCATION_MAP",
    [
        {"name": "NEW_ITEM_CODE", "type": "STRING"},
        {"name": "ITEM_ID", "type": "INTEGER"},
        {"name": "LOCATION_ID", "type": "INTEGER"},
        {"name": "DISPLAYNAME", "type": "STRING"},
        {"name": "ON_HAND_COUNT", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda *args: """
        SELECT
            NEW_ITEM_CODE,
            ITEM_ID,
            LOCATION_ID,
            i.DISPLAYNAME,
            ON_HAND_COUNT
        FROM
            "Vua Nem Joint Stock Company".Administrator.ITEM_LOCATION_MAP ilm
        LEFT JOIN
            "Vua Nem Joint Stock Company".Administrator.ITEMS i
        ON
            ilm.NEW_ITEM_CODE = i.NEW_ITEM_CODE
    """,
)
