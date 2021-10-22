from sqlalchemy import Column, Integer, DateTime, Float

from models.models import NetSuite
from components import connector
from components import getter
from components import loader


class ns2_transactionLine(NetSuite):
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["linelastmodifieddate"],
        "rank_incre_key": ["linelastmodifieddate"],
        "row_num_incre_key": ["linelastmodifieddate"],
    }
    query = """
        SELECT
            transaction AS TRANSACTION_ID,
            expenseaccount AS ACCOUNT_ID,
            netamount,
            rate,
            rateamount,
            linelastmodifieddate,
            ratepercent
        FROM
            transactionLine
        WHERE
            linelastmodifieddate >= TO_DATE('{{ start }}', 'YYYY-MM-DD HH24:MI:SS')
            AND linelastmodifieddate <= TO_DATE('{{ end }}', 'YYYY-MM-DD HH24:MI:SS')
    """
    schema = [
        {"name": "TRANSACTION_ID", "type": "INTEGER"},
        {"name": "ACCOUNT_ID", "type": "INTEGER"},
        {"name": "netamount", "type": "FLOAT"},
        {"name": "rate", "type": "FLOAT"},
        {"name": "rateamount", "type": "FLOAT"},
        {"name": "linelastmodifieddate", "type": "TIMESTAMP"},
        {"name": "ratepercent", "type": "FLOAT"},
    ]
    columns = [
        Column("TRANSACTION_ID", Integer),
        Column("ACCOUNT_ID", Integer),
        Column("netamount", Float),
        Column("rate", Float),
        Column("rateamount", Float),
        Column("linelastmodifieddate", DateTime(timezone=True)),
        Column("ratepercent", Float),
    ]
    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.PostgresIncrementalLoader,
        loader.BigQueryIncrementalLoader,
    ]
