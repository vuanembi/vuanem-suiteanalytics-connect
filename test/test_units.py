from unittest.mock import Mock

import pytest

from main import main
from models.models import TABLES
from netsuite.pipeline import (
    static_pipelines,
    time_dynamic_pipelines,
    id_dynamic_pipelines,
)
from netsuite.netsuite_service import pipeline_service

TIME_RANGE = [
    ("auto", (None, None)),
    ("manual", ("2022-01-10", "2022-01-11")),
]
ID_RANGE = [
    ("auto", (None, None)),
    ("manual", (1, 1000)),
]

TIME_START = "2022-01-01"
TIME_END = "2022-01-03"
ID_START = 1
ID_END = 1000


def parameterize(pipeline_group):
    return {
        "argnames": "pipeline",
        "argvalues": pipeline_group.values(),
        "ids": pipeline_group.keys(),
    }


class TestPipeline:
    @pytest.mark.parametrize(**parameterize(static_pipelines))
    def test_static_pipeline(self, pipeline):
        res = pipeline_service(pipeline)(None, None)
        assert res['output_rows'] > 0

    @pytest.mark.parametrize(**parameterize(time_dynamic_pipelines))
    @pytest.mark.parametrize(
        "timerange",
        [tr[1] for tr in TIME_RANGE],
        ids=[tr[0] for tr in TIME_RANGE],
    )
    def test_time_dynamic_pipeline(self, pipeline, timerange):
        res = pipeline_service(pipeline)(
            timerange[0],
            timerange[1],
        )
        assert res['output_rows'] > 0


    @pytest.mark.parametrize(**parameterize(id_dynamic_pipelines))
    @pytest.mark.parametrize(
        "idrange",
        [ir[1] for ir in ID_RANGE],
        ids=[ir[0] for ir in ID_RANGE],
    )
    def test_id_dynamic_pipeline(self, pipeline, idrange):
        res = pipeline_service(pipeline)(
            idrange[0],
            idrange[1],
        )


def process(data):
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    return res.get("results")


class TestPipelines:
    def assert_pipelines(self, res):
        assert res["num_processed"] >= 0
        if res["num_processed"] > 0:
            for i in res["loads"]:
                assert res["num_processed"] == i["output_rows"]

    @pytest.mark.parametrize(
        "table",
        TABLES["standard"],
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
            *TABLES["time_incre"],
            *TABLES["id_incre"],
        ],
    )
    @pytest.mark.timeout(0)
    def test_auto(self, table):
        data = {
            "table": table,
        }
        res = process(data)
        self.assert_pipelines(res)

    @pytest.mark.parametrize(
        "table",
        TABLES["time_incre"],
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
        TABLES["id_incre"],
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
