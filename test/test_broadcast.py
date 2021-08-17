from datetime import datetime
from unittest.mock import Mock

from main import main

from .utils import encode_data


def test_broadcast_standard():
    data = {
        "broadcast": "standard",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0


def test_broadcast_incre():
    data = {
        "broadcast": "incre",
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0
