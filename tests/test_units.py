from unittest.mock import Mock
from datetime import datetime

from main import main

from .utils import assertion, encode_data


def test_standard():
    data = {"data_source": "NetSuite", "table": "DEPARTMENTS"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_auto():
    data = {"date_source": "NetSuite", "table": "TRANSACTIONS"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_manual():
    data = {
        "data_source": "NetSuite",
        "table": "DELETED_RECORDS",
        "start": datetime(2021, 1, 1).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 1).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental2_manual():
    data = {
        "data_source": "NetSuite2",
        "table": "ns2_transactionLine",
        "start": datetime(2018, 6, 30).strftime("%Y-%m-%d"),
        "end": datetime(2018, 8, 1).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
