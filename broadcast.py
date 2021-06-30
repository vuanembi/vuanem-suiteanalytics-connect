import os
import json

from google.cloud import pubsub_v1


def get_tables():
    tables = []
    for data_source in ["NetSuite", "NetSuite2"]:
        for i in os.listdir(f"configs/{data_source}"):
            with open(f"configs/{data_source}/{i}", "r") as c:
                if json.load(c).get("keys"):
                    incre = True
                else:
                    incre = False
            tables.append(
                {"data_source": data_source, "table": i.split(".")[0], "incre": incre}
            )
    return tables


def publish(tables):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))

    for table in tables:
        message_json = json.dumps(
            {"data_source": table["data_source"], "table": table["table"]}
        )
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
    return len(tables)


def broadcast(mode="incre"):
    tables = get_tables()
    if mode == "incre":
        tables = [i for i in tables if i["incre"] is True]
    elif mode == "standard":
        tables = [i for i in tables if i["incre"] is False]
    else:
        raise NotImplementedError
    return {"message_sent": publish(tables)}
