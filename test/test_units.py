from unittest.mock import Mock
from datetime import datetime

from main import main

from .utils import assertion, encode_data

def test_standard():
    data = {
        "table": "LOCATIONS"
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get('results')
    assertion(res)

def test_incremental_auto():
    data = {
        "table": "TRANSACTIONS"
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get('results')
    assertion(res)

def test_incremental_manual():
    data = {
        "table": "TRANSACTION_LINES",
        "start": datetime(2018, 6, 30).strftime('%Y-%m-%d'),
        "end": datetime(2018, 7, 10).strftime('%Y-%m-%d')
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get('results')
    assertion(res)
