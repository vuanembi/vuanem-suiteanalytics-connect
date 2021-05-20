from unittest.mock import Mock
from datetime import datetime

from main import main

from .utils import assertion, encode_data


def test_standard():
    data = {"table": "VENDORS"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_auto():
    data = {"table": "CASES"}
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)


def test_incremental_manual():
    data = {
        "table": "TRANSACTION_LINES3",
        "start": datetime(2018, 7, 1).strftime("%Y-%m-%d"),
        "end": datetime(2018, 7, 31).strftime("%Y-%m-%d"),
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")