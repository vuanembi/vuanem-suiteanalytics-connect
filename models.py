import json
from datetime import datetime
from abc import ABCMeta, abstractmethod

from components import connector, getter, loader, pg_models


class NetSuiteFactory:
    @staticmethod
    def factory(table, start, end):
        args = (start, end)
        if table == "ACCOUNTS":
            return Accounts(*args)
        elif table == "BUDGET":
            return Budget(*args)
        elif table == "CLASSES":
            return Classes(*args)
        elif table == "CUSTOMERS":
            return Customers(*args)
        elif table == "DELIVERY_PERSON":
            return DeliveryPerson(*args)
        elif table == "DEPARTMENTS":
            return Departments(*args)
        elif table == "EMPLOYEES":
            return Employees(*args)
        elif table == "ITEMS":
            return Items(*args)
        elif table == "LOCATIONS":
            return Locations(*args)
        elif table == "SYSTEM_NOTES_PRICE":
            return SystemNotesPrice(*args)
        elif table == "VENDORS":
            return Vendors(*args)
        elif table == "ns2_promotionCode":
            return NS2PromotionCode(*args)
        elif table == "ITEM_LOCATION_MAP":
            return ItemLocationMap(*args)
        # *
        elif table == "CASES":
            return Cases(*args)
        elif table == "DELETED_RECORDS":
            return DeletedRecords(*args)
        elif table == "TRANSACTIONS":
            return Transactions(*args)
        elif table == "TRANSACTION_LINES":
            return TransactionLines(*args)
        elif table == "STORE_TRAFFIC":
            return StoreTraffic(*args)
        elif table == "SUPPORT_PERSON_MAP":
            return SupportPersonMap(*args)
        elif table == "ns2_transactionLine":
            return NS2TransactionLine(*args)
        elif table == "ns2_couponCode":
            return NS2CouponCode(*args)
        elif table == "ns2_tranPromotion":
            return NS2TranPromotion(*args)
        elif table == "LOYALTY_TRANSACTION":
            return LoyaltyTransaction(*args)
        elif table == "SERVICE_ADDON_SO_MAP":
            return ServiceAddonSOMap(*args)
        elif table == "SERVICE_ADDON_TO_MAP":
            return ServiceAddonTOMap(*args)
        # *
        else:
            raise NotImplementedError(table)


class NetSuite(metaclass=ABCMeta):
    @property
    @abstractmethod
    def table(self):
        pass

    @property
    @abstractmethod
    def connector(self):
        pass

    @property
    @abstractmethod
    def getter(self):
        pass

    @property
    @abstractmethod
    def loader(self):
        pass

    @property
    def config(self):
        with open(f"configs/{self._connector.data_source}/{self.table}.json") as f:
            config = json.load(f)
        return config

    @property
    def schema(self):
        return self.config["schema"]

    def __init__(self, start, end):
        self.start, self.end = start, end
        self._connector = self.connector()
        self._getter = self.getter(self)
        self._loader = [loader(self) for loader in self.loader]

    def _transform(self, rows):
        int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]
        for row in rows:
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
        return rows

    def run(self):
        rows = self._getter.get()
        print(datetime.now().isoformat())
        response = {
            "table": self.table,
            "data_source": self._connector.data_source,
            "num_processed": len(rows),
        }
        if getattr(self._getter, "start", None) and getattr(self._getter, "end", None):
            response["start"] = self._getter.start
            response["end"] = self._getter.end
        if len(rows) > 0:
            rows = self._transform(rows)
            print(datetime.now().isoformat())
            response["loads"] = [loader.load(rows) for loader in self._loader]
        return response


# * Standard


class Accounts(NetSuite):
    table = "ACCOUNTS"
    model = pg_models.Accounts

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Budget(NetSuite):
    table = "BUDGET"
    model = pg_models.Budget

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        # loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Classes(NetSuite):
    table = "CLASSES"
    model = pg_models.Classes

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Customers(NetSuite):
    table = "CUSTOMERS"
    model = pg_models.Customers

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class DeliveryPerson(NetSuite):
    table = "DELIVERY_PERSON"
    model = pg_models.DeliveryPerson

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Departments(NetSuite):
    table = "DEPARTMENTS"
    model = pg_models.Departments

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Employees(NetSuite):
    table = "EMPLOYEES"
    model = pg_models.Employees

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Items(NetSuite):
    table = "ITEMS"
    model = pg_models.Items

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Locations(NetSuite):
    table = "LOCATIONS"
    model = pg_models.Locations

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class SystemNotesPrice(NetSuite):
    table = "SYSTEM_NOTES_PRICE"
    model = pg_models.SystemNotesPrice

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class Vendors(NetSuite):
    table = "VENDORS"
    model = pg_models.Vendors

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class NS2PromotionCode(NetSuite):
    table = "ns2_promotionCode"
    model = pg_models.NS2_PromotionCode

    connector = connector.NetSuite2Connector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


class ItemLocationMap(NetSuite):
    table = "ITEM_LOCATION_MAP"
    model = pg_models.ItemLocationMap

    connector = connector.NetSuiteConnector
    getter = getter.StandardGetter
    loader = [
        loader.BigQueryStandardLoader,
        loader.PostgresStandardLoader,
    ]


# * Incremental


class Cases(NetSuite):
    table = "CASES"
    keys = {
        "p_key": ["CASE_ID"],
        "rank_key": ["CASE_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]


class DeletedRecords(NetSuite):
    table = "DELETED_RECORDS"
    keys = {
        "p_key": ["RECORD_ID"],
        "rank_key": ["RECORD_ID"],
        "incre_key": ["DATE_DELETED"],
        "rank_incre_key": ["DATE_DELETED"],
        "row_num_incre_key": ["DATE_DELETED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class Transactions(NetSuite):
    table = "TRANSACTIONS"

    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class TransactionLines(NetSuite):
    table = "TRANSACTION_LINES"
    keys = {
        "p_key": ["TRANSACTION_ID", "TRANSACTION_LINE_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED", "TRANSACTIONS_DATE_LAST_MODIFIED"],
        "rank_incre_key": ["TRANSACTIONS_DATE_LAST_MODIFIED"],
        "row_num_incre_key": [
            "TRANSACTIONS_DATE_LAST_MODIFIED",
            "DATE_LAST_MODIFIED",
        ],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class StoreTraffic(NetSuite):
    table = "STORE_TRAFFIC"
    keys = {
        "p_key": ["STORE_TRAFFIC_ID"],
        "rank_key": ["STORE_TRAFFIC_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class SupportPersonMap(NetSuite):
    table = "SUPPORT_PERSON_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class NS2TransactionLine(NetSuite):
    table = "ns2_transactionLine"
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["linelastmodifieddate"],
        "rank_incre_key": ["linelastmodifieddate"],
        "row_num_incre_key": ["linelastmodifieddate"],
    }
    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class NS2CouponCode(NetSuite):
    table = "ns2_couponCode"
    keys = {
        "p_key": ["id"],
        "rank_key": ["id"],
        "incre_key": ["id"],
        "rank_incre_key": ["id"],
        "row_num_incre_key": ["id"],
    }
    connector = connector.NetSuite2Connector
    getter = getter.IDIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class NS2TranPromotion(NetSuite):
    table = "ns2_tranPromotion"
    keys = {
        "p_key": ["transaction", "couponcode", "promocode"],
        "rank_key": ["transaction"],
        "incre_key": ["lastmodifieddate"],
        "rank_incre_key": ["lastmodifieddate"],
        "row_num_incre_key": ["lastmodifieddate"],
    }
    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class LoyaltyTransaction(NetSuite):
    table = "LOYALTY_TRANSACTION"
    keys = {
        "p_key": ["LOYALTY_TRANSACTION_ID"],
        "rank_key": ["LOYALTY_TRANSACTION_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class ServiceAddonSOMap(NetSuite):
    table = "SERVICE_ADDON_SO_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_SO_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]


class ServiceAddonTOMap(NetSuite):
    table = "SERVICE_ADDON_TO_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_TO_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
    ]
