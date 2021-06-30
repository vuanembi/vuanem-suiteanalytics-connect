import os
import json
from datetime import datetime, timedelta
from argparse import ArgumentParser

from google.cloud import pubsub_v1

DATE_FORMAT = "%Y-%m-%d"


def send_messages(data_source, table, start, end):
    """Send Messages to PubSub Topic

    Args:
        table (str): Table name
        start (str): Date in %Y-%m-%d
        end (str): Date in %Y-%m-%d
    """

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), "vuanem_ns")

    messages = get_time_range(data_source, table, start, end)
    print(messages)
    for i in messages:
        message_json = json.dumps(i)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()
    print(f"Sent {len(messages)} messages")


def get_time_range(data_source, table, start, end):
    """Break down time range to smaller chunks

    Args:
        table (str): Table name
        start (str): Date in %Y-%m-%d
        end (str): Date in %Y-%m-%d

    Returns:
        list: List of time ranges
    """

    start = datetime.strptime(start, DATE_FORMAT)
    end = datetime.strptime(end, DATE_FORMAT)
    _start = start
    date_array = []
    days = 10
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
        {"data_source": data_source, "table": table, "start": i[0], "end": i[1]} for i in messages if len(i) == 2
    ]
    return messages


def main():
    """Main function"""

    parser = ArgumentParser()
    parser.add_argument("--source")
    parser.add_argument("--table")
    parser.add_argument("--start")
    parser.add_argument("--end")
    args = parser.parse_args()

    _ = send_messages(args.source, args.table, args.start, args.end)


if __name__ == "__main__":
    main()
