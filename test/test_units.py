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


def assertion(res):
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        for i in res['loads']:
            assert res["num_processed"] == i["output_rows"]


@pytest.mark.parametrize(
    "table",
    [
        *TIME_TABLES,
        *ID_TABLES,
    ],
)
def test_auto(table):
    data = {
        "table": table,
    }
    res = process(data)
    assertion(res)


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
    res = process(data)
    assertion(res)


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
    res = process(data)
    assertion(res)


@pytest.mark.parametrize(
    "table",
    [
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
    ],
)
def test_standard(table):
    data = {
        "table": table,
    }
    res = process(data)
    assertion(res)
