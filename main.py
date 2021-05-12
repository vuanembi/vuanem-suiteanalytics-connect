import os
import json
import base64

import requests

from pipelines import NetSuiteJob

def main(request):
    request_json = request.get_json()
    message = request_json['message']
    data_bytes = message['data']
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if 'table' in data:
        table = data['table']
        start = data.get('start')
        end = data.get('end')
        job = NetSuiteJob.factory(table, start, end)
    else:
        job = NetSuiteJob.factory("CLASSES", None, None)

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
