import json
import base64
from unittest.mock import Mock
from datetime import datetime

from main import main

from .assertions import assertion

mock_context = Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

def test_standard():
    message = {
        "table": "CLASSES"
    }
    message_json = json.dumps(message)
    event = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(event, mock_context)
    res = res.get('results')
    assertion(res)

def test_incremental_auto():
    message = {
        "table": "TRANSACTIONS"
    }
    message_json = json.dumps(message)
    event = {"data": base64.b64encode(message_json.encode("utf-8"))}
    res = main(event, mock_context)
    res = res.get('results')
    assertion(res)

def test_incremental_manual():
    message = {
        "table": "TRANSACTIONS",
        "start": datetime(2018, 6, 30).strftime('%Y-%m-%d'),
        "end": datetime(2018, 7, 10).strftime('%Y-%m-%d')
    }
    message_bytes = json.dumps(message).encode("utf-8")
    data = {
        "data": base64.b64encode(message_bytes).decode("utf-8")
    }
    res = main(data, mock_context)
    res = res.get('results')
    assertion(res)
