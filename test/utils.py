import json
import base64

from unittest.mock import Mock

from main import main

def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] >= 0
        assert res["num_processed"] == res["output_rows"]
        assert res["errors"] is None

def process(data):
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    res = res.get("results")
    assertion(res)
