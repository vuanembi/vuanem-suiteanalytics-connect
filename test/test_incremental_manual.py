from datetime import datetime

from .utils import process

START = "2018-06-30"
END = "2018-07-02"


def test_cases():
    data = {
        "data_source": "NetSuite",
        "table": "CASES",
        "start": START,
        "end": END,
    }
    process(data)


def test_deleted_records():
    data = {
        "data_source": "NetSuite",
        "table": "DELETED_RECORDS",
        "start": START,
        "end": END,
    }
    process(data)


def test_transactions():
    data = {
        "data_source": "NetSuite",
        "table": "TRANSACTIONS",
        "start": START,
        "end": END,
    }
    process(data)


def test_transaction_lines():
    data = {
        "data_source": "NetSuite",
        "table": "TRANSACTION_LINES",
        "start": START,
        "end": END,
    }
    process(data)


def test_store_traffic():
    data = {
        "data_source": "NetSuite",
        "table": "STORE_TRAFFIC",
        "start": START,
        "end": END,
    }
    process(data)


def test_support_person_map():
    data = {
        "data_source": "NetSuite",
        "table": "SUPPORT_PERSON_MAP",
        "start": START,
        "end": END,
    }
    process(data)


def test_ns2_transactionLine():
    data = {
        "data_source": "NetSuite2",
        "table": "ns2_transactionLine",
        "start": START,
        "end": END,
    }
    process(data)


def test_ns2_couponCode():
    data = {
        "data_source": "NetSuite2",
        "table": "ns2_couponCode",
        "start": 2000000,
        "end": 2900000,
    }
    process(data)


def test_ns2_tranPromo():
    data = {
        "data_source": "NetSuite2",
        "table": "ns2_tranPromotion",
        "start": START,
        "end": datetime(2021, 7, 5).strftime("%Y-%m-%d"),
    }
    process(data)


def test_loyalty_transaction():
    data = {
        "data_source": "NetSuite",
        "table": "LOYALTY_TRANSACTION",
        "start": START,
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_service_addon_so_map():
    data = {
        "data_source": "NetSuite",
        "table": "SERVICE_ADDON_SO_MAP",
        "start": START,
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_service_addon_to_map():
    data = {
        "data_source": "NetSuite",
        "table": "SERVICE_ADDON_TO_MAP",
        "start": START,
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)


def test_system_notes():
    data = {
        "data_source": "NetSuite",
        "table": "SYSTEM_NOTES",
        "start": START,
        "end": END,
    }
    process(data)
