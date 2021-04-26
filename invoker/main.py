import os
import json

from google.cloud import pubsub_v1


def publish():
    topic_id = "vuanem_ns"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv('GCP_PROJECT'), topic_id)

    for table in [i.split(".")[0] for i in os.listdir("vuanem_ns/schemas")]:
        message_json = json.dumps(
            {
                "data": {"table": table},
            }
        )
        message_bytes = message_json.encode("utf-8")
        _ = publisher.publish(topic_path, data=message_bytes)


def main(request):
    _ = publish()
    return "ok"
