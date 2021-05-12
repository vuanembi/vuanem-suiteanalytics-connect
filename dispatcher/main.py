import os
import json
import base64

from google.cloud import pubsub_v1


def publish():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv('PROJECT_ID'), "vuanem_ns")

    with open("tables.json", "r") as f:
        tables = json.load(f).get("tables")

    for table in tables:
        message_json = json.dumps(
            {
                "data": {"table": table},
            }
        )
        message_bytes = message_json.encode("utf-8")
        data = base64.b64encode(message_bytes)
        _ = publisher.publish(topic_path, data=data)


def main(request):
    _ = publish()
    return "ok"
