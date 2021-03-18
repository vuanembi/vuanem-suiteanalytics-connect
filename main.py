import os
import json

import requests
import pyodbc
import pandas as pd
from google.cloud import bigquery
from pexecute.thread import ThreadLoom


class NetSuiteJob:
    def __init__(self, table):
        self.dataset = "NetSuite"
        self.table = table

    def connect_ns(self):
        return pyodbc.connect(
            DSN=os.getenv("NS_DSN"),
            UID=os.getenv("NS_UID"),
            PWD=os.getenv("NS_PWD"),
        )

    def extract(self):
        with open(f"queries/{self.table}.sql") as f:
            query = f.read()
        df = pd.read_sql(sql=query, con=self.connect_ns())
        self.num_processed = df.shape[0]
        return df

    def transform(self, df):
        return df

    def load(self, df):
        with open(f"schemas/{self.table}.json") as f:
            schema = json.load(f)

        client = bigquery.Client()

        return client.load_table_from_dataframe(
            df,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_TRUNCATE",
            ),
        ).result()

    def run(self):
        df = self.extract()
        df = self.transform(df)
        errors = self.load(df)
        return {
            "table": self.table,
            "num_processed": self.num_processed,
            "output_rows": errors.output_rows,
            "errors": errors.errors,
        }

def main():
    SalesOrderLines = NetSuiteJob("SalesOrderLines")
    InventoryMovements = NetSuiteJob("InventoryMovements")

    loom = ThreadLoom(max_runner_cap=10)
    for i in [SalesOrderLines, InventoryMovements]:
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
