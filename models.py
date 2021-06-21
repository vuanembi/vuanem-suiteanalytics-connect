import os
import json
from datetime import datetime
from abc import ABCMeta, abstractmethod

import jinja2
import jaydebeapi
from google.cloud import bigquery

DATASET = "NetSuite"
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

QUERIES_LOADER = jinja2.FileSystemLoader(searchpath="./queries")
QUERIES_ENV = jinja2.Environment(loader=QUERIES_LOADER)

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
ROLE_ID = os.getenv("ROLE_ID")
USER = os.getenv("NS_UID")
PWD = os.getenv("NS_PWD")

BQ_CLIENT = bigquery.Client()


class NetSuite(metaclass=ABCMeta):
    """Semi-abstract class for NetSuite Job

    Args:
        metaclass (abc.ABCMeta, optional): Abstract Class. Defaults to ABCMeta.
    """

    def __init__(self, data_source, table):
        """Initiate NetSuite Job

        Args:
            data_source (str): Data Source
            table (str): Table Name
        """

        self.data_source = data_source
        self.table = table
        with open(f"configs/{data_source}/{table}.json") as f:
            self.config = json.load(f)
        self.schema = self.config["schema"]

    @staticmethod
    def factory(data_source, table, start, end):        
        """Factory Method for creating NetSuiteJob

        Args:
            data_source (str): Data Source
            table (str): Table Name
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            NetSuite: NetSuite pipelines
        """

        query_path = f"queries/{data_source}/{table}.sql.j2"
        with open(query_path, "r") as q:
            query = q.read()

        if "{{" in query:
            return NetSuiteIncremental(data_source, table, start, end)
        else:
            return NetSuiteStandard(data_source, table)

    def connect_ns(self):
        """Connect NetSuite using JDBC

        Returns:
            jaydebeapi.Connection: JDBC Connection
        """

        account_id, role_id, user, pwd = self._get_credentials()
        return jaydebeapi.connect(
            "com.netsuite.jdbc.openaccess.OpenAccessDriver",
            (
                f"jdbc:ns://{account_id}.connect.api.netsuite.com:1708;"
                f"ServerDataSource={self.data_source}.com;"
                "Encrypted=1;"
                f"CustomProperties=(AccountID={account_id};RoleID={role_id})"
            ),
            {"user": user, "password": pwd},
            "NQjc.jar",
        )

    def _get_credentials(self):
        account_id = os.getenv("ACCOUNT_ID")
        if self.data_source == "NetSuite":
            role_id = os.getenv("ROLE_ID")
            user = os.getenv("NS_UID")
            pwd = os.getenv("NS_PWD")
        elif self.data_source == "NetSuite2":
            role_id = os.getenv("ROLE_ID2")
            user = os.getenv("NS_UID2")
            pwd = os.getenv("NS_PWD2")
        return account_id, role_id, user, pwd

    def extract(self):
        """Extract data using SQL from NetSuite

        Returns:
            rows: List of results in JSON
        """

        conn = self.connect_ns()
        cursor = conn.cursor()
        query = self._build_query()
        cursor.execute(query)

        columns = [column[0] for column in cursor.description]
        rows = []
        while True:
            results = cursor.fetchmany(50000)
            if results:
                rows.extend([dict(zip(columns, result)) for result in results])
            else:
                break

        self.num_processed = len(rows)

        cursor.close()
        conn.close()
        return rows

    def _build_query(self):
        template = QUERIES_ENV.get_template(f"{self.data_source}/{self.table}.sql.j2")
        rendered_query = template.render(
            start=getattr(self, "start", None), end=getattr(self, "end", None)
        )
        return rendered_query

    @abstractmethod
    def _get_cursor(self, cursor):
        """Get Cursor

        Args:
            cursor (jaydebeapi.Cursor): JDBC Cursor

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def transform(self, rows):
        """Transform data extracted from NetSuite

        Args:
            rows (list): List of results in JSON

        Returns:
            list: List of results in JSON
        """

        int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]
        for row in rows:
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def load(self, rows):
        """Load data to staging table on BigQuery

        Args:
            rows (list): List of results in JSON

        Returns:
            google.cloud.bigquery.job.base_AsyncJob: LoadJob Results
        """

        load_target = self._get_load_target()
        write_disposition = self._get_write_disposition()
        loads = BQ_CLIENT.load_table_from_json(
            rows,
            load_target,
            job_config=bigquery.LoadJobConfig(
                schema=self.schema,
                create_disposition="CREATE_IF_NEEDED",
                write_disposition=write_disposition,
            ),
        ).result()

        del rows
        return loads

    @abstractmethod
    def _get_load_target(self):
        raise NotImplementedError

    @abstractmethod
    def _get_write_disposition(self):
        """Get Write Disposition

        Raises:
            NotImplementedError: Abstract Method
        """
        raise NotImplementedError

    @abstractmethod
    def _update(self):
        """Update Main table using DDL

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    def run(self):
        """Main function to start the job

        Returns:
            dict: Job Results
        """

        rows = self.extract()
        if len(rows) == 0:
            responses = {"table": self.table, "num_processed": self.num_processed}
        else:
            rows = self.transform(rows)
            loads = self.load(rows)
            self._update()
            responses = {
                "table": self.table,
                "num_processed": self.num_processed,
                "output_rows": loads.output_rows,
                "errors": loads.errors,
            }
            responses = self._make_responses(responses)
            BQ_CLIENT.close()
            return responses

    @abstractmethod
    def _make_responses(self):
        """Abstract Method to make responses

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError


class NetSuiteStandard(NetSuite):
    def __init__(self, data_source, table):
        """Inititate Standard Job

        Args:
            data_source (str): Data Source
            table (str): Table Name
        """

        super().__init__(data_source, table)

    def _get_cursor(self, cursor):
        cursor.execute(self.query)
        return cursor

    def _get_write_disposition(self):
        write_disposition = "WRITE_TRUNCATE"
        return write_disposition

    def _get_load_target(self):
        return f"{DATASET}.{self.table}"

    def _update(self):
        pass

    def _make_responses(self, responses):
        return responses


class NetSuiteIncremental(NetSuite):
    def __init__(self, data_source, table, start, end):
        """Inititate Incremental Job

        Args:
            data_source (str): Data Source
            table (str): Table Name
            keys (dict): Keys for DDL
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """

        super().__init__(data_source, table)
        self.keys = self.config.get("keys")
        self.start, self.end = self._get_time_range(start, end)

    def _get_time_range(self, start, end):
        """Get Start & End Date
        If no start & end specified, defaults to latest value got from the main table

        Args:
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            tuple: (start, end)
        """

        if start and end:
            start, end = [
                datetime.strptime(i, DATE_FORMAT).strftime(TIMESTAMP_FORMAT)
                for i in [start, end]
            ]
        else:
            now = datetime.utcnow()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = self._get_latest_incre()
        return start, end

    def _get_latest_incre(self):
        """Get latest incremental value

        Returns:
            str: Latest incremental Value
        """

        template = TEMPLATE_ENV.get_template(f"read_max_incremental.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            incremental_key=self.keys["incremental_key"],
        )
        rows = BQ_CLIENT.query(rendered_query).result()
        row = [row for row in rows][0]
        max_incre = row.get("incre")
        return max_incre.strftime(TIMESTAMP_FORMAT)

    def _get_cursor(self, cursor):
        """Execute SQL with Params

        Args:
            cursor (jaydebeapi.Cursor): Cursor

        Returns:
            jaydebeapi.Cursor: Cursor after Execution
        """

        cursor.execute(self.query, [self.start, self.end])
        return cursor

    def _get_write_disposition(self):
        return "WRITE_APPEND"

    def _get_load_target(self):
        return f"{DATASET}._stage_{self.table}"

    def _update(self):
        """Update Procedure"""

        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            p_key=self.keys["p_key"],
            rank_key=self.keys["rank_key"],
            row_num_incremental_key=self.keys["row_num_incremental_key"],
            rank_incremental_key=self.keys["rank_incremental_key"],
        )
        rendered_query
        _ = BQ_CLIENT.query(rendered_query).result()

    def _make_responses(self, responses):
        """Make responses for Job Result

        Args:
            responses (dict): Initial Responses

        Returns:
            dict: Responses
        """

        responses["start"] = self.start
        responses["end"] = self.end
        return responses
