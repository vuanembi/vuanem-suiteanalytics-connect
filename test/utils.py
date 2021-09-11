import json
import base64

from unittest.mock import Mock

from main import main


def process(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    message = {
        "message": {
            "data": data_encoded,
        },
    }
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    return res.get("results")
