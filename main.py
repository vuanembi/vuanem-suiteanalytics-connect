import os
import json
import base64

import requests

from models import NetSuiteFactory
from broadcast import broadcast


def main(request):
    """Gateway to process request

    Args:
        request (flask.Request): Request from PubSub

    Returns:
        dict: Responses
    """

    request_json = request.get_json()
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if "broadcast" in data:
        responses = broadcast(data["broadcast"])
    else:
        job = NetSuiteFactory.factory(
            data['table'],
            data.get("start"),
            data.get("end"),
        )
        responses = {
            "pipelines": "NetSuite",
            "results": job.run(),
        }

    print(responses)

    requests.post(
        f"https://api.telegram.org/bot{os.getenv('TELEGRAM_TOKEN')}/sendMessage",
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": json.dumps(responses, indent=4),
        },
    )
    return responses
