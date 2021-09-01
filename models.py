import components

class NetSuiteFactory:
    @staticmethod
    def factory(table, start, end):
        args = (start, end)
        if table == 'ACCOUNTS':
            return Accounts(*args)
        elif table == 'TRANSACTIONS':
            return Transactions(*args)
        elif table == 'ns2_couponCode':
            return NS2CouponCode(*args)
        else:
            raise NotImplementedError(table)


class Accounts(components.NetSuite):
    table = "ACCOUNTS"
    keys = {}

    connector = components.NetSuiteConnector
    getter = components.StandardGetter
    loader = [components.BigQueryStandardLoader]

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
