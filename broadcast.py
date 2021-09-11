import os
import json

from google.cloud import pubsub_v1

from models import TABLES

PUBLISHER = pubsub_v1.PublisherClient()
TOPIC_PATH = PUBLISHER.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))


def broadcast(data):
    mode = data['broadcast']
    if mode == "incre":
        tables = [*TABLES["time_incre"], *TABLES["id_incre"]]
    elif mode == "standard":
        tables = TABLES["standard"]
    else:
        raise NotImplementedError
    tables = [k for i in tables for k in i.keys()]
    for table in tables:
        message_json = json.dumps(
            {
                "table": table,
            }
        )
        message_bytes = message_json.encode("utf-8")
        PUBLISHER.publish(TOPIC_PATH, data=message_bytes).result()
    return {
        "mode": mode,
        "message_sent": len(tables),
    }
