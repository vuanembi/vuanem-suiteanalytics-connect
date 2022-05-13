import argparse

from datetime import datetime
from dateutil.rrule import rrule, MONTHLY

from netsuite.netsuite_controller import netsuite_controller


def parse_datetime(start, end) -> list[datetime]:
    return [datetime.strptime(i, "%Y-%m-%d") for i in [start, end]]


def generate_range(start: datetime, end: datetime):
    start_of_months = [i for i in rrule(MONTHLY, dtstart=start, until=end)]
    return [start_of_months[i : i + 2] for i in range(len(start_of_months))]


def main(name, start, end):
    date_ranges = [i for i in generate_range(*parse_datetime(start, end)) if len(i) > 1]

    for date_range in date_ranges:
        print(date_range)
        res = netsuite_controller(
            {
                "table": name,
                "start": date_range[0],
                "end": date_range[1],
            }
        )
        print(res)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("name")
    parser.add_argument("start")
    parser.add_argument("end")

    args = parser.parse_args()

    print(args)

    main(args.name, args.start, args.end)
