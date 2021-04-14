import os
import json
from argparse import ArgumentParser

import requests
from tqdm import tqdm

from pipelines import NetSuiteJob

def main():
    pipelines = [i.split('.')[0] for i in os.listdir('schemas')]
    jobs = [NetSuiteJob(i) for i in pipelines]

    responses = {
            "pipelines": "NetSuite",
            "results": [i.run() for i in tqdm(jobs)]
        }

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
