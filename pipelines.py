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

J2_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
J2_ENV = jinja2.Environment(loader=J2_LOADER)


class NetSuiteJob(metaclass=ABCMeta):
    """Semi-abstract class for NetSuite Job

    Args:
        metaclass (abc.ABCMeta, optional): Abstract Class. Defaults to ABCMeta.
    """  

    def __init__(self, table, query, schema):
        """Initiate NetSuite Job

        Args:
            table (str): Table Name
            query (str): SQL Query
            schema (list): Schema in JSON
        """

        self.table = table
        self.query = query
        self.schema = schema
        self.client = bigquery.Client()

    @staticmethod
    def factory(table, start, end):
        """Factory Method for creating NetSuiteJob

        Args:
            table (str): Table Name
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d

        Returns:
            NetSuiteJob: NetSuiteJob
        """

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
        """Connect NetSuite using JDBC

        Returns:
            jaydebeapi.Connection: JDBC Connection
        """

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
        """Extract data using SQL from NetSuite

        Returns:
            rows: List of results in JSON
        """

        conn = self.connect_ns()
        cursor = conn.cursor()
        cursor = self._fetch_cursor(cursor)

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

    @abstractmethod
    def _fetch_cursor(self, cursor):
        """Abstract Method for cursor execution

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
        """Abstract Method to get Write Disposition

        Raises:
            NotImplementedError: Abstract Method
        """        
        raise NotImplementedError

    @abstractmethod
    def _update(self):
        """Abstract Method to get Update procedure

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError

    @abstractmethod
    def _fetch_updated_query(self):
        """Abstract Method to get Update DDL

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
            return responses

    @abstractmethod
    def _make_responses(self):
        """Abstract Method to make responses

        Raises:
            NotImplementedError: Abstract Method
        """

        raise NotImplementedError


class NetSuiteStandardJob(NetSuiteJob):
    def __init__(self, table, query, schema):
        """Inititate Standard Job

        Args:
            table (str): Table Name
            query (str): SQL Query
            schema (list): Schema in JSON
        """

        super().__init__(table, query, schema)

    def _fetch_cursor(self, cursor):
        """Execute SQL without Params

        Args:
            cursor (jaydebeapi.Cursor): Cursor

        Returns:
            jaydebeapi.Cursor: Cursor after Execution
        """

        cursor.execute(self.query)
        return cursor

    def _fetch_write_disposition(self):
        """Fetch Write Disposition as Truncate

        Returns:
            str: Write Disposition
        """

        write_disposition = "WRITE_TRUNCATE"
        return write_disposition

    def _update(self):
        """Update procedure to standard table"""        

        rendered_query = self._fetch_updated_query()
        _ = self.client.query(rendered_query).result()

    def _fetch_updated_query(self):
        """Fetch Update DDL

        Returns:
            str: Update DDL
        """

        template = J2_ENV.get_template("update.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
        )
        return rendered_query

    def _make_responses(self, responses):
        """Make Responses for Job Results

        Args:
            responses (dict): Default responses

        Returns:
            dict: Responses
        """

        return responses


class NetSuiteIncrementalJob(NetSuiteJob):
    def __init__(self, table, query, schema, keys, start, end):
        """Inititate Incremental Job

        Args:
            table (str): Table Name
            query (str): SQL Query
            schema (list): Schema in JSON
            keys (dict): Keys for DDL
            start (str): Date in %Y-%m-%d
            end (str): Date in %Y-%m-%d
        """

        super().__init__(table, query, schema)
        self.keys = keys
        self.start, self.end = self._fetch_time_range(start, end)

    def _fetch_time_range(self, start, end):
        """Fetch Start & End Date
        If no start & end specified, defaults to latest value fetched from the main table

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
            self.manual = True
        else:
            now = datetime.utcnow()
            end = now.strftime(TIMESTAMP_FORMAT)
            start = self._fetch_latest_incre()
            self.manual = False
        return start, end

    def _fetch_latest_incre(self):
        """Fetch latest incremental value

        Returns:
            str: Latest incremental Value
        """

        template = J2_ENV.get_template("query_max_incremental.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=self.table,
            incremental_key=self.keys["incremental_key"],
        )
        rows = self.client.query(rendered_query).result()
        row = [row for row in rows][0]
        max_incre = row.get("incre")
        return max_incre.strftime(TIMESTAMP_FORMAT)

    def _fetch_cursor(self, cursor):
        """Execute SQL with Params

        Args:
            cursor (jaydebeapi.Cursor): Cursor

        Returns:
            jaydebeapi.Cursor: Cursor after Execution
        """

        cursor.execute(self.query, [self.start, self.end])
        return cursor

    def _fetch_write_disposition(self):
        """Fetch Write Disposition

        Returns:
            str: Write Disposition
        """

        return "WRITE_APPEND"

    def _update(self):
        """Update Procedure"""

        if not self.manual:
            rendered_query = self._fetch_updated_query()
            _ = self.client.query(rendered_query).result()
        else:
            pass

    def _fetch_updated_query(self):
        """Fetch Update DDL

        Returns:
            str: Update DDL
        """

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
        """Make responses for Job Result

        Args:
            responses (dict): Initial Responses

        Returns:
            dict: Responses
        """

        responses["start"] = self.start
        responses["end"] = self.end
        return responses
