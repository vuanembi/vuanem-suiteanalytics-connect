import os
import json
from datetime import datetime, timedelta

import jinja2
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
            self.schema_fields = self.schema.get("schema")
            self.schema_keys = self.schema.get("keys")

        if "?" in self.query:
            self.full_sync = full_sync
            self.incremental = True
        else:
            self.full_sync = True
            self.incremental = False

        self.date_cols = [i["name"] for i in self.schema_fields if i["type"] == "DATE"]
        self.timestamp_cols = [
            i["name"] for i in self.schema_fields if i["type"] == "TIMESTAMP"
        ]
        self.int_cols = [
            i["name"] for i in self.schema_fields if i["type"] == "INTEGER"
        ]

        self.client = bigquery.Client()

    def connect_ns(self):
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                "jdbc:ns://{ACCOUNT_ID}.connect.api.netsuite.com:1708;"
                "ServerDataSource=NetSuite.com;"
                "Encrypted=1;"
                "CustomProperties=(AccountID={ACCOUNT_ID};RoleID={ROLE_ID})"
            ).format(ACCOUNT_ID=os.getenv("ACCOUNT_ID"), ROLE_ID=os.getenv("ROLE_ID")),
            {"user": os.getenv("NS_UID"), "password": os.getenv("NS_PWD")},
            "NQjc.jar",
        )

    def extract(self):
        cursor = self.connect_ns().cursor()

        if self.incremental == True and self.full_sync == False:
            self.cutoff = (datetime.now() - timedelta(days=1)).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            cursor.execute(self.query, [self.cutoff])
        elif self.incremental == True and self.full_sync == True:
            self.cutoff = "2018-06-30 00:00:00"
            cursor.execute(self.query, [self.cutoff])
        else:
            cursor.execute(self.query)

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
        if self.incremental == True and self.full_sync == False:
            write_disposition = "WRITE_APPEND"
        else:
            write_disposition = "WRITE_TRUNCATE"

        errors = self.client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema_fields,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        del rows
        return errors

    def update(self):
        loader = jinja2.FileSystemLoader(searchpath="./templates")
        env = jinja2.Environment(loader=loader)

        if self.incremental == True:
            template = env.get_template("update_incremental.sql.j2")
            rendered_query = template.render(
                dataset=self.dataset,
                table=self.table,
                p_key=self.keys.get("p_key"),
                incremental_key=self.keys.get("incremental_key"),
                partition_key=self.keys.get("partition_key"),
            )
        else:
            template = env.get_template("update.sql.j2")
            rendered_query = template.render(
                dataset=self.dataset,
                table=self.table,
            )

        _ = self.client.query(rendered_query).result()

    def run(self):
        rows = self.extract()
        rows = self.transform(rows)
        errors = self.load(rows)
        _ = self.update()
        return {
            "table": self.table,
            "incremental": self.incremental,
            "full_sync": self.full_sync,
            "num_processed": self.num_processed,
            "output_rows": errors.output_rows,
            "errors": errors.errors,
        }


def main(request):
    job = NetSuiteJob("CLASSES")
    return job.run()


if __name__ == "__main__":
    main({})
