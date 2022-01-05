import os
import json

import requests

from models.models import NetSuite
from tasks import create_tasks


def main(request):
    """Gateway to process request

    Args:
        request (flask.Request): HTTP Request

    Returns:
        dict: Responses
    """

    data = request.get_json()
    print(data)

    if "mode" in data and "table" not in data:
        results = create_tasks(data)
    else:
        results = NetSuite.factory(
            data["table"],
            data.get("start"),
            data.get("end"),
        ).run()

    responses = {
        "pipelines": "NetSuite",
        "results": results,
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
