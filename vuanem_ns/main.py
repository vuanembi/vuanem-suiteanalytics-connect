import os
import json
import base64

import requests

from pipelines import NetSuiteJob


def main(data, context):
    message = json.loads(base64.b64decode(data).decode('utf-8'))

    if 'table' in message:
        table = message['table']
        full_sync = message.get('full_sync')
        if full_sync:
            job = NetSuiteJob(table, full_sync)
        else:
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
