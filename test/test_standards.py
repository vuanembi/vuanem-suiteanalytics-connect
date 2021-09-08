import pytest

from .utils import process


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
    process(data)
