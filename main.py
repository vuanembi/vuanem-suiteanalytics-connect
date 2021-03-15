import os
import json
from datetime import datetime

import requests
import pyodbc
import pandas as pd
from google.cloud import bigquery


class NetSuiteJob:
    def __init__(self, table):
        self.table = table
        self.dataset = "NetSuite"

    def connect(self):
        return pyodbc.connect(
            DSN=os.getenv("NS_DSN"),
            UID=os.getenv("NS_UID"),
            PWD=os.getenv("NS_PWD"),
        )

    def extract(self):
        with open(f"queries/{self.table}.sql") as f:
            self.query = f.read()
        df = pd.read_sql(sql=self.query, con=self.connect())
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
            "num_processed": self.num_processed,
            "output_rows": errors.output_rows,
            "errors": errors.errors,
        }


def main(request):
    print(datetime.now())
    SalesOrderLines = NetSuiteJob("SalesOrderLines")
    InventoryMovements = NetSuiteJob("InventoryMovements")
    results = {
        "pipelines": "NetSuite",
        "results": [
            SalesOrderLines.run(),
            InventoryMovements.run()
        ],
    }

    print(results)

    _ = requests.post(
        "https://api.telegram.org/bot{token}/sendMessage".format(
            token=os.getenv("TELEGRAM_TOKEN")
        ),
        json={
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "text": json.dumps(results, indent=4),
        },
    )
    print(datetime.now())


if __name__ == "__main__":
    main(0)
