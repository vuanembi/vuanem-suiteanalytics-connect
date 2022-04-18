from netsuite.pipeline.interface import Pipeline, Key
from netsuite.repo import netsuite2_connection
from db.bigquery import id_builder, update

pipeline = Pipeline(
    "ns2_couponCode",
    [
        {"name": "code", "type": "STRING"},
        {"name": "datesent", "type": "TIMESTAMP"},
        {"name": "externalid", "type": "STRING"},
        {"name": "id", "type": "INTEGER"},
        {"name": "promotion", "type": "INTEGER"},
    ],
    conn_fn=netsuite2_connection,
    query_fn=lambda ir: f"""
        SELECT
            code,
            datesent,
            externalid,
            id,
            promotion
        FROM
            couponCode cc
        WHERE
            id >= {ir[0]}
            AND id <= {ir[1]}
    """,
    param_fn=id_builder,
    key=Key(
        id_key=["id"],
        rank_key=["id"],
        cursor_key=["id"],
        cursor_rank_key=["id"],
        cursor_rn_key=["id"],
    ),
    load_callback_fn=update,
)
