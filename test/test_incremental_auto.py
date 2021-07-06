from .utils import process


def test_cases():
    data = {"data_source": "NetSuite", "table": "CASES"}
    process(data)


def test_deleted_records():
    data = {"data_source": "NetSuite", "table": "DELETED_RECORDS"}
    process(data)


def test_transactions():
    data = {"data_source": "NetSuite", "table": "TRANSACTIONS"}
    process(data)


def test_transaction_lines():
    data = {"data_source": "NetSuite", "table": "TRANSACTION_LINES"}
    process(data)


def test_store_traffic():
    data = {"data_source": "NetSuite", "table": "STORE_TRAFFIC"}
    process(data)


def test_support_person_map():
    data = {
        "data_source": "NetSuite",
        "table": "SUPPORT_PERSON_MAP",
    }
    process(data)


def test_ns2_transactionLine():
    data = {"data_source": "NetSuite2", "table": "ns2_transactionLine"}
    process(data)


def test_ns2_couponCode():
    data = {"data_source": "NetSuite2", "table": "ns2_couponCode"}
    process(data)


def test_ns2_tranPromo():
    data = {"data_source": "NetSuite2", "table": "ns2_tranPromotion"}
    process(data)
