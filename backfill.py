import os
import json
from datetime import datetime, timedelta
from argparse import ArgumentParser

from google.cloud import pubsub_v1


def send_messages(start, end):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "vuanem_ns")

    messages = get_time_range(start, end)
    print(messages)
    for i in messages:
        message_json = json.dumps(i)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
    print(f"Sent {len(messages)} messages")


def get_time_range(start, end):
    start = datetime.strptime(start, "%Y-%m-%d")
    end = datetime.strptime(end, "%Y-%m-%d")
    _start = start
    date_array = []
    while _start <= end:
        date_array.append(_start.strftime("%Y-%m-%d"))
        _start = _start + timedelta(days=30)
        if _start >= end:
            date_array.append(end.strftime("%Y-%m-%d"))
    messages = [date_array[i : i + 2] for i in range(len(date_array))]
    messages = [
        {"table": "TRANSACTIONS", "start": i[0], "end": i[1]}
        for i in messages
        if len(i) == 2
    ]
    return messages


def main():
    parser = ArgumentParser()
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    _ = send_messages(args.start, args.end)


if __name__ == "__main__":
    main()
