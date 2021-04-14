import os
import json
from datetime import datetime, timedelta

import requests
import pyodbc
from tqdm import tqdm
from google.cloud import bigquery
from pexecute.thread import ThreadLoom


class NetSuiteJob:
    dataset = "NetSuite"

    def __init__(self, table, full_sync=False):
        self.table = table
        self.full_sync = full_sync

        with open(f"queries/{table}.sql", "r") as f:
            self.query = f.read()

        with open(f"schemas/{table}.json", "r") as f:
            self.schema = json.load(f)

        self.date_cols = [i["name"] for i in self.schema if i["type"] == "DATE"]
        self.timestamp_cols = [
            i["name"] for i in self.schema if i["type"] == "TIMESTAMP"
        ]
        self.int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]

    def connect_ns(self):
        return pyodbc.connect(
            DSN=os.getenv("NS_DSN"),
            UID=os.getenv("NS_UID"),
            PWD=os.getenv("NS_PWD"),
        )

    def extract(self):
        cursor = self.connect_ns().cursor()
        if self.full_sync == True:
            self.cutoff = "2018-06-30 00:00:00"
        else:
            self.cutoff = (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        cursor.execute(self.query, self.cutoff)
        columns = [column[0] for column in cursor.description]
        rows = []
        while True:
            results = cursor.fetchmany(200)
            if results:
                rows.extend((dict(zip(columns, result)) for result in results))
            else:
                break
        self.num_processed = len(rows)
        return rows

    def transform(self, rows):
        for row in tqdm(rows):
            if self.date_cols:
                for col in self.date_cols:
                    row[col] = (
                        row[col].strftime("%Y-%m-%d")
                        if row[col] is not None
                        else row[col]
                    )
            if self.timestamp_cols:
                for col in self.timestamp_cols:
                    row[col] = (
                        row[col].strftime("%Y-%m-%d %H:%M:%S")
                        if row[col] is not None
                        else row[col]
                    )
            if self.int_cols:
                for col in self.int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def load(self, rows):
        client = bigquery.Client()
        
        if self.full_sync == True:
            write_disposition="WRITE_TRUNCATE"
        else:
            write_disposition="WRITE_APPEND"

        return client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

    def send_report(self, errors):
        responses = {
            "pipelines": "NetSuite",
            "results": {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": errors.output_rows,
                "errors": errors.errors,
            },
        }

        print(responses)

        _ = requests.post(
            "https://api.telegram.org/bot{token}/sendMessage".format(
                token=os.getenv("TELEGRAM_TOKEN")
            ),
            json={
                "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
                "text": json.dumps(responses, indent=4),
            },
        )
        return responses

    def run(self):
        rows = self.extract()
        rows = self.transform(rows)
        errors = self.load(rows)
        return self.send_report(errors)


def main(request):
    job = NetSuiteJob("TRANSACTIONS")
    job.run()


if __name__ == "__main__":
    main({})
