import os
import json
import base64

from google.cloud import pubsub_v1


def publish():
    topic_id = "vuanem_ns"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("voltaic-country-280607", topic_id)

    # with open("tables.json", "r") as f:
    #     tables = json.load(f).get("tables")

    for table in ['CLASSES']:
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

main({})
