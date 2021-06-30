from datetime import datetime

from .utils import process


def test_cases():
    data = {
        "data_source": "NetSuite",
        "table": "CASES",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_deleted_records():
    data = {
        "data_source": "NetSuite",
        "table": "DELETED_RECORDS",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_transactions():
    data = {
        "data_source": "NetSuite",
        "table": "TRANSACTIONS",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_transaction_lines():
    data = {
        "data_source": "NetSuite",
        "table": "TRANSACTION_LINES",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_store_traffic():
    data = {
        "data_source": "NetSuite",
        "table": "STORE_TRAFFIC",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_ns2_transactionLine():
    data = {
        "data_source": "NetSuite2",
        "table": "ns2_transactionLine",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)
