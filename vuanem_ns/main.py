import os
import json
import base64

import requests

from pipelines import NetSuiteJob


def main(event, context):
    if 'data' in event:
        message = base64.b64decode(event['data']).decode('utf-8')
        if 'table' in message:
            table = message['table']
            job = NetSuiteJob(table)
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
