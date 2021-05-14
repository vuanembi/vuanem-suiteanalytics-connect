import os
import json
import base64

from google.cloud import pubsub_v1


def main(request):
    request_json = request.get_json(force=True, silent=True)
    print(request_json)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "vuanem_ns")

    with open("tables.json", "r") as f:
        tables = json.load(f)

    if request_json.get("mode") == "incre":
        tables = [table["table"] for table in tables if table["incre"] is True]
    else:
        tables = [table["table"] for table in tables if table["incre"] is False]

    _ = publish(publisher, topic_path, tables)
    responses = {"message_sent": len(tables)}
    print(responses)
    return responses


def publish(publisher, topic_path, tables):
    for table in tables:
        message_json = json.dumps({"table": table})
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
