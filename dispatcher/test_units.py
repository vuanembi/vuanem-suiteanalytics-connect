from unittest.mock import Mock
from datetime import datetime

from main import main

def test_incre():
    data = {"mode": "incre"}
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res['message_sent'] > 0
