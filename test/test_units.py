from unittest.mock import Mock

import pytest

from main import main

STANDARD_TABLES = [
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
]
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
    "LOYALTY_CUSTOMER_GROUP",
]
ID_TABLES = [
    "ns2_couponCode",
]

TIME_START = "2018-06-01"
TIME_END = "2021-09-16"
ID_START = 1
ID_END = 1000


def process(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res.get("results")


class TestPipelines:
    def assert_pipelines(res):
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            for i in res["loads"]:
                assert res["num_processed"] == i["output_rows"]

    @pytest.mark.parametrize(
        "table",
        STANDARD_TABLES,
    )
    def test_standard(self, table):
        data = {
            "table": table,
        }
        res = process(data)
        self.assert_pipelines(res)

    @pytest.mark.parametrize(
        "table",
        [
            *TIME_TABLES,
            *ID_TABLES,
        ],
    )
    def test_auto(self, table):
        data = {
            "table": table,
        }
        res = process(data)
        self.assert_pipelines(res)

    @pytest.mark.parametrize(
        "table",
        TIME_TABLES,
    )
    @pytest.mark.timeout(0)
    def test_manual_time(self, table):
        data = {
            "table": table,
            "start": TIME_START,
            "end": TIME_END,
        }
        res = process(data)
        self.assert_pipelines(res)

    @pytest.mark.parametrize(
        "table",
        ID_TABLES,
    )
    def test_manual_id(self, table):
        data = {
            "table": table,
            "start": ID_START,
            "end": ID_END,
        }
        res = process(data)
        self.assert_pipelines(res)


@pytest.mark.parametrize(
    "mode",
    [
        "standard",
        "incre",
    ],
)
def test_tasks(mode):
    res = process(
        {
            "mode": mode,
        }
    )
    assert res["tasks"] > 0
