import re
import importlib
from abc import ABCMeta, abstractmethod


class NetSuite(metaclass=ABCMeta):
    @staticmethod
    def factory(table, start, end):
        try:
            tables = [i for v in TABLES.values() for i in v if i == table]
            table_matched = tables[0]
            module = importlib.import_module(f"models.{table_matched}")
            model = getattr(module, table_matched)
            return model(start, end)
        except (ImportError, AttributeError, IndexError):
            raise ValueError(table)

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

    def __init__(self, start, end):
        self.start, self.end = start, end
        self.table = self.__class__.__name__
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
            # if str_cols:
            #     for col in str_cols:
            #         row[col] = re.sub(pattern, "", row[col]) if row[col] else None
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


TABLES = {
    "standard": [
        "ACCOUNTS",
        "BUDGET",
        "CLASSES",
        "DELIVERY_PERSON",
        "DEPARTMENTS",
        "EMPLOYEES",
        "ITEMS",
        "LOCATIONS",
        "SYSTEM_NOTES_PRICE",
        "VENDORS",
        "ns2_promotionCode",
        "ITEM_LOCATION_MAP",
        "CAMPAIGNS",
        "TRANSACTIONS_DUE_DATE",
        "PURCHASE_ORDER",
        "IR",
        "CASETYPE",
        "ns2_promotionCodeCurrency",
    ],
    "time_incre": [
        "CASES",
        "CUSTOMERS",
        "DELETED_RECORDS",
        "TRANSACTIONS",
        "TRANSACTION_LINES",
        "STORE_TRAFFIC",
        "SUPPORT_PERSON_MAP",
        "ns2_transactionLine",
        "ns2_tranPromotion",
        "LOYALTY_TRANSACTION",
        "SERVICE_ADDON_SO_MAP",
        "SERVICE_ADDON_TO_MAP",
        "PROMOTION_SMS_INTEGRATION",
        "LOYALTY_CUSTOMER_GROUP",
        "SYSTEM_NOTES_CREATE",
        "ORIGINATING_LEADS",
        "ns2_transaction",
        "RATING",
        "ENTITY",
        "ADDRESSES",
        "ADDRESS_BOOK",
        "CITYPROVINCE_LIST",
        "QUANHUYEN_LIST",
    ],
    "id_incre": [
        "ns2_couponCode",
    ],
}
