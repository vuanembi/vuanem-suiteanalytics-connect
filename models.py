import json
from datetime import datetime
import re
from abc import ABCMeta, abstractmethod

from components import connector, getter, loader, pg_models


class NetSuiteFactory:
    @staticmethod
    def factory(table, start, end):
        tables_mapping = [i for v in TABLES.values() for i in v]
        tables =  [v for i in tables_mapping for k, v in i.items() if k == table]
        if len(tables) == 1:
            table = tables[0]
            return table(start, end)
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
        pattern = "[\t\n\r\f\v]"
        int_cols = [i["name"] for i in self.schema if i["type"] == "INTEGER"]
        str_cols = [i["name"] for i in self.schema if i["type"] == "STRING"]
        for row in rows:
            if int_cols:
                for col in int_cols:
                    row[col] = int(row[col]) if row[col] is not None else row[col]
            if str_cols:
                for col in str_cols:
                    row[col] = re.sub(pattern, "", row[col]) if row[col] else None
        return rows

    def run(self):
        rows = self._getter.get()
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
        loader.BigQueryStandardLoader,
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
    model = pg_models.NS2PromotionCode

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


class Customers(NetSuite):
    table = "CUSTOMERS"
    model = pg_models.Customers
    keys = {
        "p_key": ["CUSTOMER_ID"],
        "rank_key": ["CUSTOMER_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    model = pg_models.Customers

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]


class Cases(NetSuite):
    table = "CASES"
    model = pg_models.Cases
    keys = {
        "p_key": ["CASE_ID"],
        "rank_key": ["CASE_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    model = pg_models.Cases

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
    model = pg_models.DeletedRecords

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.Transactions

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]


class TransactionLines(NetSuite):
    table = "TRANSACTION_LINES"
    keys = {
        "p_key": ["TRANSACTION_ID", "TRANSACTION_LINE_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED", "TRANSACTIONS_DATE_LAST_MODIFIED"],
        "rank_incre_key": ["TRANSACTIONS_DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["TRANSACTIONS_DATE_LAST_MODIFIED", "DATE_LAST_MODIFIED"],
    }
    model = pg_models.TransactionLines

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.StoreTraffic

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.SupportPersonMap

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.NS2TransactionLine

    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.NS2CouponCode
    connector = connector.NetSuite2Connector
    getter = getter.IDIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.NS2TranPromotion

    connector = connector.NetSuite2Connector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.LoyaltyTransaction

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.ServiceAddonSOMap

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
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
    model = pg_models.ServiceAddonTOMap

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]


class PromotionSMSIntegration(NetSuite):
    table = "PROMOTION_SMS_INTEGRATION"
    keys = {
        "p_key": ["PROMOTION_SMS_INTEGRATION_ID"],
        "rank_key": ["PROMOTION_SMS_INTEGRATION_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    model = pg_models.PromotionSMSIntegration

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]


class LoyaltyCustomerGroup(NetSuite):
    table = "LOYALTY_CUSTOMER_GROUP"
    keys = {
        "p_key": ["LOYALTY_CUSTOMER_GROUP_ID"],
        "rank_key": ["LOYALTY_CUSTOMER_GROUP_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    model = pg_models.LoyaltyCustomerGroup

    connector = connector.NetSuiteConnector
    getter = getter.TimeIncrementalGetter
    loader = [
        loader.BigQueryIncrementalLoader,
        loader.PostgresIncrementalLoader,
    ]



TABLES = {
    "standard": [
        {"ACCOUNTS": Accounts},
        {"BUDGET": Budget},
        {"CLASSES": Classes},
        {"DELIVERY_PERSON": DeliveryPerson},
        {"DEPARTMENTS": Departments},
        {"EMPLOYEES": Employees},
        {"ITEMS": Items},
        {"LOCATIONS": Locations},
        {"SYSTEM_NOTES_PRICE": SystemNotesPrice},
        {"VENDORS": Vendors},
        {"ns2_promotionCode": NS2PromotionCode},
        {"ITEM_LOCATION_MAP": ItemLocationMap},
    ],
    "time_incre": [
        {"CASES": Cases},
        {"CUSTOMERS": Customers},
        {"DELETED_RECORDS": DeletedRecords},
        {"TRANSACTIONS": Transactions},
        {"TRANSACTION_LINES": TransactionLines},
        {"STORE_TRAFFIC": StoreTraffic},
        {"SUPPORT_PERSON_MAP": SupportPersonMap},
        {"ns2_transactionLine": NS2TransactionLine},
        {"ns2_tranPromotion": NS2TranPromotion},
        {"LOYALTY_TRANSACTION": LoyaltyTransaction},
        {"SERVICE_ADDON_SO_MAP": ServiceAddonSOMap},
        {"SERVICE_ADDON_TO_MAP": ServiceAddonTOMap},
        {"PROMOTION_SMS_INTEGRATION": PromotionSMSIntegration},
        {"LOYALTY_CUSTOMER_GROUP": LoyaltyCustomerGroup},
    ],
    "id_incre": [{"ns2_couponCode": NS2CouponCode}],
}
