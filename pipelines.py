import os
import json
from datetime import datetime, timedelta

import jaydebeapi
from tqdm import tqdm
from google.cloud import bigquery


class NetSuiteJob:
    dataset = "NetSuite"

    def __init__(self, table, full_sync=False):
        self.table = table

        with open(f"queries/{table}.sql", "r") as q, open(
            f"schemas/{table}.json", "r"
        ) as s:
            self.query = q.read()
            self.schema = json.load(s)

        if "?" in self.query:
            self.full_sync = full_sync
            self.no_params = False
        else:
            self.full_sync = True
            self.no_params = True

        self.date_cols = [i["name"] for i in self.schema if i["type"] == "DATE"]
        self.timestamp_cols = [
            i["name"] for i in self.schema if i["type"] == "TIMESTAMP"
        ]
        self.int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]

    def connect_ns(self):
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                "jdbc:ns://{ACCOUNT_ID}.connect.api.netsuite.com:1708;"
                "ServerDataSource=NetSuite.com;"
                "Encrypted=1;"
                "CustomProperties=(AccountID={ACCOUNT_ID};RoleID={ROLE_ID})"
            ).format(ACCOUNT_ID=4975572, ROLE_ID=1022),
            {"user": os.getenv("NS_UID"), "password": os.getenv("NS_PWD")},
            "NQjc.jar",
        )

    def extract(self):
        cursor = self.connect_ns().cursor()

        if self.full_sync == True:
            self.cutoff = "2018-06-30 00:00:00"
        else:
            self.cutoff = (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        if self.no_params == True:
            cursor.execute(self.query)
        else:
            cursor.execute(self.query, [self.cutoff])

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
            if self.int_cols:
                for col in self.int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def load(self, rows):
        client = bigquery.Client()
        if self.full_sync == True:
            write_disposition = "WRITE_TRUNCATE"
        else:
            write_disposition = "WRITE_APPEND"

        errors = client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        del rows
        return errors

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
    job = NetSuiteJob("TRANSACTIONS")
    return job.run()


if __name__ == "__main__":
    main({})
