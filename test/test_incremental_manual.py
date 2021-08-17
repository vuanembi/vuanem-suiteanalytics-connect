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


def test_support_person_map():
    data = {
        "data_source": "NetSuite",
        "table": "SUPPORT_PERSON_MAP",
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
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 5).strftime("%Y-%m-%d"),
    }
    process(data)


def test_loyalty_transaction():
    data = {
        "data_source": "NetSuite",
        "table": "LOYALTY_TRANSACTION",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)

def test_service_addon_so_map():
    data = {
        "data_source": "NetSuite",
        "table": "SERVICE_ADDON_SO_MAP",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)

def test_service_addon_to_map():
    data = {
        "data_source": "NetSuite",
        "table": "SERVICE_ADDON_TO_MAP",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)

def test_system_notes():
    data = {
        "data_source": "NetSuite",
        "table": "SYSTEM_NOTES",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 2).strftime("%Y-%m-%d"),
    }
    process(data)
