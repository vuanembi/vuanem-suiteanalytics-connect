import os
import json
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod

import jinja2
import jaydebeapi
from google.cloud import bigquery

DATASET = "NetSuite"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

J2_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
J2_ENV = jinja2.Environment(loader=J2_LOADER)


class NetSuiteJob(metaclass=ABCMeta):
    def __init__(self, table, query, schema):
        self.table = table
        self.query = query
        self.schema = schema
        self.client = bigquery.Client()

    @staticmethod
    def factory(table, start, end):
        query_path = f"queries/{table}.sql"
        config_path = f"config/{table}.json"
        with open(query_path, "r") as q, open(config_path, "r") as s:
            query = q.read()
            config = json.load(s)
            schema = config.get("schema")
            keys = config.get("keys")

        if "?" in query:
            return NetSuiteIncrementalJob(table, query, schema, keys, start, end)
        else:
            return NetSuiteStandardJob(table, query, schema)

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
        conn = self.connect_ns()
        cursor = conn.cursor()
        cursor = self._fetch_cursor(cursor)

        columns = [column[0] for column in cursor.description]
        rows = []
        while True:
            results = cursor.fetchmany(50000)
            if results:
                rows.extend((dict(zip(columns, result)) for result in results))
            else:
                break

        self.num_processed = len(rows)

        cursor.close()
        conn.close()
        return rows

    @abstractmethod
    def _fetch_cursor(self, cursor):
        raise NotImplementedError

    def transform(self, rows):
        int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]
        for row in rows:
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def load(self, rows):
        write_disposition = self._fetch_write_disposition()
        loads = self.client.load_table_from_json(
            rows,
            f"{DATASET}._stage_{self.table}",
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        del rows
        return loads

    @abstractmethod
    def _fetch_write_disposition(self):
        raise NotImplementedError

    def update(self):
        rendered_query = self._fetch_updated_query()
        _ = self.client.query(rendered_query).result()

    @abstractmethod
    def _fetch_updated_query(self):
        raise NotImplementedError

    def run(self):
        rows = self.extract()
        rows = self.transform(rows)
        loads = self.load(rows)
        self.update()

        responses = {
            "table": self.table,
            "num_processed": self.num_processed,
            "output_rows": loads.output_rows,
            "errors": loads.errors,
        }
        responses = self._make_responses(responses)
        return responses

    @abstractmethod
    def _make_responses(self):
        raise NotImplementedError


class NetSuiteStandardJob(NetSuiteJob):
    def __init__(self, table, query, schema):
        super().__init__(table, query, schema)

    def _fetch_cursor(self, cursor):
        cursor.execute(self.query)
        return cursor

    def _fetch_write_disposition(self):
        write_disposition = "WRITE_TRUNCATE"
        return write_disposition

    def _fetch_updated_query(self):
        template = J2_ENV.get_template("update.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
        )
        return rendered_query

    def _make_responses(self, responses):
        return responses


class NetSuiteIncrementalJob(NetSuiteJob):
    def __init__(self, table, query, schema, keys, start, end):
        self.keys = keys
        self.start, self.end = self._fetch_time_range(start, end)
        super().__init__(table, query, schema)

    def _fetch_time_range(self, start, end):
        if start and end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [start, end]
            ]
        else:
            now = datetime.now()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = (now - timedelta(days=3)).strftime(TIMESTAMP_FORMAT)
        return start, end

    def _fetch_cursor(self, cursor):
        cursor.execute(self.query, [self.start, self.end])
        return cursor

    def _fetch_write_disposition(self):
        return "WRITE_APPEND"

    def _fetch_updated_query(self):
        template = J2_ENV.get_template("update_incremental.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=",".join(self.keys["p_key"]),
            incremental_key=self.keys["incremental_key"],
            partition_key=self.keys["partition_key"],
        )
        return rendered_query

    def _make_responses(self, responses):
        responses["start"] = self.start
        responses["end"] = self.end
        return responses


def main():
    job = NetSuiteJob.factory("DELIVERY_PERSON")
    return job.run()


if __name__ == "__main__":
    main()