import pytest

from .utils import process

TABLES = [
    # "CASES",
    # "DELETED_RECORDS",
    "TRANSACTIONS",
    # "TRANSACTION_LINES",
    # "STORE_TRAFFIC",
    # "SUPPORT_PERSON_MAP",
    # "ns2_transactionLine",
    # "ns2_couponCode",
    # "ns2_tranPromotion",
    # "LOYALTY_TRANSACTION",
    # "SERVICE_ADDON_SO_MAP",
    # "SERVICE_ADDON_TO_MAP",
]

START = "2018-06-30"
END = "2018-07-02"


@pytest.mark.parametrize(
    "table",
    TABLES,
)
def test_auto(table):
    data = {
        "table": table,
    }
    process(data)


@pytest.mark.parametrize(
    "table",
    TABLES,
)
def test_manual(table):
    data = {
        "table": table,
        "start": START,
        "end": END,
    }
    process(data)
