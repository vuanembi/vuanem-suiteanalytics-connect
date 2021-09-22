import os
import json
import uuid

from google.cloud import tasks_v2

from models.models import TABLES


TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = {
    "project": os.getenv("PROJECT_ID"),
    "location": "asia-southeast2",
    "queue": "vuanem-ns-tasks",
}
PARENT = TASKS_CLIENT.queue_path(**CLOUD_TASKS_PATH)


def create_tasks(data):
    mode = data["mode"]
    if mode == "incre":
        tables = [*TABLES["time_incre"], *TABLES["id_incre"]]
    elif mode == "standard":
        tables = TABLES["standard"]
    else:
        raise ValueError(mode)

    payloads = [
        {
            "table": table,
            "start": data.get("start"),
            "end": data.get("end"),
        }
        for table in tables
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                **CLOUD_TASKS_PATH, task=f"{payload['table']}-{uuid.uuid4()}"
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "tasks": len(responses),
        "data": data,
    }
