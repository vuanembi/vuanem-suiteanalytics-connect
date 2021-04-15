import os
import json

import requests

from pipelines import NetSuiteJob


def main(request):
    request_json = request.get_json()
    if request_json:
        job = NetSuiteJob(request_json.get("table"))
    else:
        job = NetSuiteJob("CLASSES")

    responses = {"pipelines": "NetSuite", "results": [job.run()]}

    _ = requests.post(
        "https://api.telegram.org/bot{token}/sendMessage".format(
            token=os.getenv("TELEGRAM_TOKEN")
        ),
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": json.dumps(responses, indent=4),
        },
    )
    return responses
