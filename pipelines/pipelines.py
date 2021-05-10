import os
import json
from datetime import datetime, timedelta

import jinja2
import jaydebeapi
from tqdm import tqdm
from google.cloud import bigquery

DATASET = "NetSuite"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

J2_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
J2_ENV = jinja2.Environment(loader=J2_LOADER)


class NetSuiteJob:
    table = None
    query = None
    schema = None
    
    def __init__(self):
        self.client = bigquery.Client()

    def connect_ns(self):
        ACCOUNT_ID = os.getenv("ACCOUNT_ID")
        ROLE_ID = os.getenv("ROLE_ID")
        USER = os.getenv("NS_UID")
        PWD = os.getenv("NS_PWD")
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                f"jdbc:ns://{ACCOUNT_ID}.connect.api.netsuite.com:1708;"
                "ServerDataSource=NetSuite.com;"
                "Encrypted=1;"
                f"CustomProperties=(AccountID={ACCOUNT_ID};RoleID={ROLE_ID})"
            ),
            {"user": USER, "password": PWD},
            "NQjc.jar",
        )

    def extract(self):
        cursor = self.connect_ns().cursor()
        cursor = self._fetch_cursor(cursor)

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

    def _fetch_cursor(self, cursor):
        cursor.execute(self.query)
        return cursor

    def transform(self, rows):
        int_cols = [
            i["name"] for i in self.schema if i["type"] == "INTEGER"
        ]
        for row in tqdm(rows):
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def load(self, rows):
        write_disposition = self._fetch_write_disposition()
        loads = self.client.load_table_from_json(
            rows,
            f"{self.dataset}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema_fields,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        del rows
        return loads

    def _fetch_write_disposition(self):
        write_disposition = "WRITE_TRUNCATE"
        return write_disposition

    def update(self):
        rendered_query = self._fetch_updated_query()
        _ = self.client.query(rendered_query).result()

    def _fetch_updated_query(self):
        template = J2_ENV.get_template("update.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
        )
        return rendered_query

    def run(self):
        rows = self.extract()
        rows = self.transform(rows)
        loads = self.load(rows)
        _ = self.update()
        return {
            "table": self.table,
            "incremental": self.incremental,
            "full_sync": self.full_sync,
            "num_processed": self.num_processed,
            "output_rows": loads.output_rows,
            "errors": loads.errors,
        }


class NetSuiteIncrementalJob(NetSuiteJob):
    def __init__(self, table, full_sync=False):
        pass

    def _fetch_cursor(self, cursor):
        if self.full_sync == False:
            cutoff = (datetime.now() - timedelta(days=1)).strftime(
                TIMESTAMP_FORMAT
            )
        elif self.full_sync == True:
            cutoff = datetime(2018, 6, 30, 0, 0, 0).strftime(TIMESTAMP_FORMAT)
        cursor.execute(self.query, [cutoff])
        return cursor

    def _fetch_write_disposition(self):
        if self.full_sync == False:
            write_disposition = "WRITE_APPEND"
        else:
            write_disposition = "WRITE_TRUNCATE"
        return write_disposition

    def _fetch_update_query(self):
        template = J2_ENV.get_template("update_incremental.sql.j2")
        rendered_query = template.render(
            dataset=self.dataset,
            table=self.table,
            p_key=self.keys.get("p_key"),
            incremental_key=self.keys.get("incremental_key"),
            partition_key=self.keys.get("partition_key"),
        )
        return rendered_query


def main(request):
    job = NetSuiteJob("EMPLOYEES")
    return job.run()


if __name__ == "__main__":
    main({})
