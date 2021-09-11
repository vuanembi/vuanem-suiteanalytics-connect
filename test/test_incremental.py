import pytest

from .utils import process

TIME_TABLES = [
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
]
ID_TABLES = [
    "ns2_couponCode",
]

TIME_START = "2020-01-01"
TIME_END = "2020-07-01"
ID_START = 1
ID_END = 1000


@pytest.mark.parametrize(
    "table",
    [*TIME_TABLES, *ID_TABLES],
)
def test_auto(table):
    data = {
        "table": table,
    }
    process(data)


@pytest.mark.parametrize(
    "table",
    TIME_TABLES,
)
def test_manual_time(table):
    data = {
        "table": table,
        "start": TIME_START,
        "end": TIME_END,
    }
    process(data)


@pytest.mark.parametrize(
    "table",
    ID_TABLES,
)
def test_manual_id(table):
    data = {
        "table": table,
        "start": ID_START,
        "end": ID_END,
    }
    process(data)
