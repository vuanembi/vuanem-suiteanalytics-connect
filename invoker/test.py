import json
import base64
import requests

message = {"table": "CLASSES"}
message_bytes = base64.b64encode(
        json.dumps(message).encode("utf-8")
        ).decode('utf-8')
data = {
    "data": message_bytes
        }
r = requests.post("http://localhost:8080", json=data)
