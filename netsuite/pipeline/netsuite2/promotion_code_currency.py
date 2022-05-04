from netsuite.pipeline.interface import Pipeline
from netsuite.repo import netsuite2_connection

pipeline = Pipeline(
    "ns2_promotionCodeCurrency",
    [
        {"name": "currency", "type": "INTEGER"},
        {"name": "minimumorderamount", "type": "FLOAT"},
        {"name": "promotioncode", "type": "INTEGER"},
    ],
    netsuite2_connection,
    query_fn=lambda *args: """
        SELECT
            currency,
	        minimumorderamount,
	        promotioncode
        FROM
            promotionCodeCurrency
    """,
)
