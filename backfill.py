import os
import json
from datetime import datetime, timedelta
from argparse import ArgumentParser

from google.cloud import pubsub_v1

DATE_FORMAT = "%Y-%m-%d"


def send_messages(table, start, end):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "vuanem_ns")

    messages = get_time_range(table, start, end)
    print(messages)
    for i in messages:
        message_json = json.dumps(i)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
    print(f"Sent {len(messages)} messages")


def get_time_range(table, start, end):
    start = datetime.strptime(start, DATE_FORMAT)
    end = datetime.strptime(end, DATE_FORMAT)
    _start = start
    date_array = []
    days = 15
    if start + timedelta(days=days) > end:
        messages = [[i.strftime(DATE_FORMAT) for i in [start, end]]]
    else:
        while _start < end:
            date_array.append(_start.strftime(DATE_FORMAT))
            _start = _start + timedelta(days=days)
            if _start >= end:
                date_array.append(end.strftime(DATE_FORMAT))
        messages = [date_array[i : i + 2] for i in range(len(date_array))]
        
    messages = [
        {"table": table, "start": i[0], "end": i[1]} for i in messages if len(i) == 2
    ]
    return messages


def main():
    parser = ArgumentParser()
    parser.add_argument("--table")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    _ = send_messages(args.table, args.start, args.end)


if __name__ == "__main__":
    main()
