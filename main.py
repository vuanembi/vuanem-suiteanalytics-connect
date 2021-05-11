import os
import json
import base64

import requests

from pipelines import NetSuiteJob


def main(event, context):
    print(event)
    data = event["data"]
    print(data)
    message = json.loads(base64.b64decode(data).decode("utf-8"))
    print(message)

    if 'table' in message:
        table = message['table']
        start = message.get('start')
        end = message.get('end')
        job = NetSuiteJob.factory(table, start, end)
    else:
        job = NetSuiteJob.factory("CLASSES")

    responses = {"pipelines": "NetSuite", "results": job.run()}

    print(responses)

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
