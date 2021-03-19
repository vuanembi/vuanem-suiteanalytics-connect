import os
import json

import requests
import pyodbc
from tqdm import tqdm
from google.cloud import bigquery
from pexecute.thread import ThreadLoom


class NetSuiteJob:
    def __init__(self, table, **kwargs):
        self.table = table
        self.date_cols = kwargs.get("date_cols", None)
        self.datetime_cols = kwargs.get("datetime_cols", None)
        self.dataset = "NetSuite"

    def connect_ns(self):
        return pyodbc.connect(
            DSN=os.getenv("NS_DSN"),
            UID=os.getenv("NS_UID"),
            PWD=os.getenv("NS_PWD"),
        )

    def extract(self):
        cursor = self.connect_ns().cursor()
        with open(f"queries/{self.table}.sql") as f:
            query = f.read()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        results = cursor.fetchall()
        rows = [dict(zip(columns, result)) for result in tqdm(results)]
        self.num_processed = len(rows)
        return rows

    def transform(self, rows):
        for row in tqdm(rows):
            if self.date_cols:
                for col in self.date_cols:
                    row[col] = row[col].strftime("%Y-%m-%d")
            if self.datetime_cols:
                for col in self.datetime_cols:
                    row[col] = row[col].strftime("%Y-%m-%d %H:%M:%S")
        return rows

    def load(self, rows):
        with open(f"schemas/{self.table}.json") as f:
            schema = json.load(f)
        client = bigquery.Client()

        return client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
            ),
        ).result()

    def run(self):
        rows = self.extract()
        rows = self.transform(rows)
        errors = self.load(rows)
        return {
            "table": self.table,
            "num_processed": self.num_processed,
            "output_rows": errors.output_rows,
            "errors": errors.errors,
        }


def main(request):
    SalesOrderLines = NetSuiteJob("SalesOrderLines", date_cols=["TRANDATE"])
    # InventoryMovements = NetSuiteJob("InventoryMovements")

    loom = ThreadLoom(max_runner_cap=10)
    for i in [
        SalesOrderLines,
        # InventoryMovements
    ]:
        loom.add_function(i.run)
    results = loom.execute()

    responses = {
        "pipelines": "NetSuite",
        "results": [i["output"] for i in results.values()],
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

if __name__ == '__main__':
    main(0)
