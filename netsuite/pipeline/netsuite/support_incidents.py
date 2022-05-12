from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "SUPPORT_INCIDENTS",
    [
        {"name": "CASE_ID", "type": "INTEGER"},
        {"name": "CASE_NUMBER", "type": "INTEGER"},
        {"name": "CASE_TYPE_ID", "type": "INTEGER"},
        {"name": "CREATE_DATE", "type": "TIMESTAMP"},
        {"name": "NAME", "type": "STRING"},
        {"name": "SAO_CHO_NVBH_ID", "type": "INTEGER"},
        {"name": "SO_LOCATION_ID", "type": "INTEGER"},
        {"name": "SO_REFERENCE_ID", "type": "INTEGER"},
        {"name": "STATUS", "type": "STRING"},
        {"name": "RATING__SAU_KHI_LN_SO_ID", "type": "INTEGER"},
        {"name": "DATE_LAST_MODIFIED", "type": "TIMESTAMP"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
        SELECT
            CASE_ID,
            CASE_NUMBER,
            CASE_TYPE_ID,
            CREATE_DATE ,
            NAME,
            SAO_CHO_NVBH_ID,
            SO_LOCATION_ID,
            SO_REFERENCE_ID,
            STATUS,
            RATING__SAU_KHI_LN_SO_ID,
            DATE_LAST_MODIFIED
        FROM
            "Vua Nem Joint Stock Company".Administrator.SUPPORT_INCIDENTS
        WHERE
            DATE_LAST_MODIFIED >= '{tr[0]}'
            AND DATE_LAST_MODIFIED <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["CASE_ID"],
        cursor_key=["DATE_LAST_MODIFIED"],
    ),
    load_callback_fn=update,
)
