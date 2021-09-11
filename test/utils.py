import json
import base64

from unittest.mock import Mock

from main import main


def assertion(res):
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        for i in res['loads']:
            assert res["num_processed"] == i["output_rows"]



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
    res = res.get("results")
    assertion(res)
