import components
import pg_models


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


# * Standard


class Accounts(components.NetSuite):
    table = "ACCOUNTS"
    model = pg_models.Accounts

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Budget(components.NetSuite):
    table = "BUDGET"
    model = pg_models.Budget

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        # components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Classes(components.NetSuite):
    table = "CLASSES"
    model = pg_models.Classes

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Customers(components.NetSuite):
    table = "CUSTOMERS"
    model = pg_models.Customers

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class DeliveryPerson(components.NetSuite):
    table = "DELIVERY_PERSON"
    model = pg_models.DeliveryPerson

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Departments(components.NetSuite):
    table = "DEPARTMENTS"
    model = pg_models.Departments

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Employees(components.NetSuite):
    table = "EMPLOYEES"
    model = pg_models.Employees

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Items(components.NetSuite):
    table = "ITEMS"
    model = pg_models.Items

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Locations(components.NetSuite):
    table = "LOCATIONS"
    model = pg_models.Locations

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class SystemNotesPrice(components.NetSuite):
    table = "SYSTEM_NOTES_PRICE"
    model = pg_models.SystemNotesPrice

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class Vendors(components.NetSuite):
    table = "VENDORS"
    model = pg_models.Vendors

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class NS2PromotionCode(components.NetSuite):
    table = "ns2_promotionCode"
    model = pg_models.NS2_PromotionCode

    connector = components.NetSuite2Connector
    getter = components.StandardGetter
    loader = [
        components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


class ItemLocationMap(components.NetSuite):
    table = "ITEM_LOCATION_MAP"
    model = pg_models.ItemLocationMap

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [
        # components.BigQueryStandardLoader,
        components.PostgresStandardLoader,
    ]


# * Incremental


class Cases(components.NetSuite):
    table = "CASES"
    keys = {
        "p_key": ["CASE_ID"],
        "rank_key": ["CASE_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class DeletedRecords(components.NetSuite):
    table = "DELETED_RECORDS"
    keys = {
        "p_key": ["RECORD_ID"],
        "rank_key": ["RECORD_ID"],
        "incre_key": ["DATE_DELETED"],
        "rank_incre_key": ["DATE_DELETED"],
        "row_num_incre_key": ["DATE_DELETED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class Transactions(components.NetSuite):
    table = "TRANSACTIONS"

    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class TransactionLines(components.NetSuite):
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
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class StoreTraffic(components.NetSuite):
    table = "STORE_TRAFFIC"
    keys = {
        "p_key": ["STORE_TRAFFIC_ID"],
        "rank_key": ["STORE_TRAFFIC_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class SupportPersonMap(components.NetSuite):
    table = "SUPPORT_PERSON_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class NS2TransactionLine(components.NetSuite):
    table = "ns2_transactionLine"
    keys = {
        "p_key": ["TRANSACTION_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["linelastmodifieddate"],
        "rank_incre_key": ["linelastmodifieddate"],
        "row_num_incre_key": ["linelastmodifieddate"],
    }
    connector = components.NetSuite2Connector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class NS2CouponCode(components.NetSuite):
    table = "ns2_couponCode"
    keys = {
        "p_key": ["id"],
        "rank_key": ["id"],
        "incre_key": ["id"],
        "rank_incre_key": ["id"],
        "row_num_incre_key": ["id"],
    }
    connector = components.NetSuite2Connector
    getter = components.IDIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class NS2TranPromotion(components.NetSuite):
    table = "ns2_tranPromotion"
    keys = {
        "p_key": ["transaction", "couponcode", "promocode"],
        "rank_key": ["transaction"],
        "incre_key": ["lastmodifieddate"],
        "rank_incre_key": ["lastmodifieddate"],
        "row_num_incre_key": ["lastmodifieddate"],
    }
    connector = components.NetSuite2Connector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class LoyaltyTransaction(components.NetSuite):
    table = "LOYALTY_TRANSACTION"
    keys = {
        "p_key": ["LOYALTY_TRANSACTION_ID"],
        "rank_key": ["LOYALTY_TRANSACTION_ID"],
        "incre_key": ["LAST_MODIFIED_DATE"],
        "rank_incre_key": ["LAST_MODIFIED_DATE"],
        "row_num_incre_key": ["LAST_MODIFIED_DATE"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class ServiceAddonSOMap(components.NetSuite):
    table = "SERVICE_ADDON_SO_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_SO_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class ServiceAddonTOMap(components.NetSuite):
    table = "SERVICE_ADDON_TO_MAP"
    keys = {
        "p_key": ["TRANSACTION_ID", "LIST_SERVICE_ADD_ON_TO_ID"],
        "rank_key": ["TRANSACTION_ID"],
        "incre_key": ["DATE_LAST_MODIFIED"],
        "rank_incre_key": ["DATE_LAST_MODIFIED"],
        "row_num_incre_key": ["DATE_LAST_MODIFIED"],
    }
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]
