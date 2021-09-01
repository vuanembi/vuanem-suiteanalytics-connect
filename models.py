import components


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
        # ---
        else:
            raise NotImplementedError(table)


class Accounts(components.NetSuite):
    table = "ACCOUNTS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Budget(components.NetSuite):
    table = "BUDGET"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Classes(components.NetSuite):
    table = "CLASSES"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Customers(components.NetSuite):
    table = "CUSTOMERS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class DeliveryPerson(components.NetSuite):
    table = "DELIVERY_PERSON"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Departments(components.NetSuite):
    table = "DEPARTMENTS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Employees(components.NetSuite):
    table = "EMPLOYEES"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Items(components.NetSuite):
    table = "ITEMS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Locations(components.NetSuite):
    table = "LOCATIONS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class SystemNotesPrice(components.NetSuite):
    table = "SYSTEM_NOTES_PRICE"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class Vendors(components.NetSuite):
    table = "VENDORS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class NS2PromotionCode(components.NetSuite):
    table = "ns2_promotionCode"
    keys = {}

    connector = components.NetSuite2Connector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


class ItemLocationMap(components.NetSuite):
    table = "ITEM_LOCATION_MAP"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]


# -------------------------------------------------------------------------------------------------------


class Transactions(components.NetSuite):
    table = "TRANSACTIONS"
    connector = components.NetSuiteConnector
    getter = components.TimeIncrementalGetter
    loader = [components.BigQueryIncrementalLoader]


class NS2CouponCode(components.NetSuite):
    table = "ns2_couponCode"
    connector = components.NetSuite2Connector
    getter = components.IDIncrementalGetter
    loader = [components.BigQueryStandardLoader]
