from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite_connection
from db.bigquery import timeframe_builder, update

pipeline = Pipeline(
    "CITYPROVINCE_LIST",
    [
        {"name": "CITYPROVINCE_LIST_EXTID", "type": "STRING"},
        {"name": "CITYPROVINCE_LIST_ID", "type": "INTEGER"},
        {"name": "CITYPROVINCE_LIST_NAME", "type": "STRING"},
        {"name": "DATE_CREATED", "type": "TIMESTAMP"},
        {"name": "IS_INACTIVE", "type": "STRING"},
        {"name": "LAST_MODIFIED_DATE", "type": "TIMESTAMP"},
        {"name": "PARENT_ID", "type": "INTEGER"},
    ],
    conn_fn=netsuite_connection,
    query_fn=lambda tr: f"""
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
            LAST_MODIFIED_DATE >= '{tr[0]}'
            AND LAST_MODIFIED_DATE <= '{tr[1]}'
    """,
    param_fn=timeframe_builder,
    key=Key(
        id_key=["CITYPROVINCE_LIST_ID"],
        rank_key=["CITYPROVINCE_LIST_ID"],
        cursor_key=["LAST_MODIFIED_DATE"],
        cursor_rank_key=["LAST_MODIFIED_DATE"],
        cursor_rn_key=["LAST_MODIFIED_DATE"],
    ),
    load_callback_fn=update,
)
