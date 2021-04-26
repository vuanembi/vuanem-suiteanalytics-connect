import json
import base64
from unittest.mock import Mock

from main import main

mock_context = Mock()
mock_context.event_id = '617187464135194'
mock_context.timestamp = '2019-07-15T22:09:03.761Z'

def test_normal():
    message = {
        "table": "CLASSES"
    }
    message_bytes = json.dumps(message).encode("utf-8")
    data = {
        "data": base64.b64encode(message_bytes).decode("utf-8")
    }
    res = main(data, mock_context)
    for i in res.get('results'):
        assert i['num_processed'] > 0
        assert i['output_rows'] > 0
        assert i['num_processed'] == i['output_rows']
        assert i['errors'] is None
