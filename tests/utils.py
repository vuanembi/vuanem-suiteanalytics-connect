import json
import base64

def encode_data(data):
    data_json = json.dumps(data)
    data_encoded = base64.b64encode(data_json.encode("utf-8"))
    return {"message": {"data": data_encoded}}

def assertion(res):
    assert res["num_processed"] > 0
    assert res["output_rows"] > 0
    assert res["num_processed"] == res["output_rows"]
    assert res["errors"] is None
