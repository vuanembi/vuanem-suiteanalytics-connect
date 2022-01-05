from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_promotionCodeCurrency(NetSuite):
    query = """
        SELECT
            currency,
	        minimumorderamount,
	        promotioncode
        FROM
            promotionCodeCurrency
    """
    schema = [
        {"name": "currency", "type": "INTEGER"},
        {"name": "minimumorderamount", "type": "FLOAT"},
        {"name": "promotioncode", "type": "INTEGER"},
    ]
    columns = []
    connector = connector.NetSuite2Connector
    getter = getter.StandardGetter
    loader = [
        # loader.PostgresStandardLoader,
        loader.BigQueryStandardLoader,
    ]
