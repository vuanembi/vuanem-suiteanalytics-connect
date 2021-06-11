from unittest.mock import Mock
from datetime import datetime

from main import main

from .utils import assertion, encode_data


def test_standard():
    data = {"table": "DEPARTMENTS"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_auto():
    data = {"table": "TRANSACTIONS"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_manual():
    data = {
        "table": "DELETED_RECORDS",
        "start": datetime(2021, 1, 1).strftime("%Y-%m-%d"),
        "end": datetime(2021, 7, 1).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
