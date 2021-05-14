import os
import json
import base64

from google.cloud import pubsub_v1


def publish(publisher, topic_path, tables):
    for table in tables:
        message_json = json.dumps({"table": table})
        message_bytes = message_json.encode("utf-8")
        data = base64.b64encode(message_bytes)
        publisher.publish(topic_path, data=data).result()


def main(request):
    request_json = request.get_json()
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "vuanem_ns")

    with open("tables.json", "r") as f:
        tables = json.load(f).get("tables")

    if request_json.get("mode") == "incre":
        tables = [table["table"] for table in tables if table["incre"] is True]

    _ = publish(publisher, topic_path, tables)
    return {"message_sent": len(tables)}
