from unittest.mock import Mock
from datetime import date, timedelta

import pytest

from netsuite.pipeline import (
    static_pipelines,
    time_dynamic_pipelines,
    id_dynamic_pipelines,
)
from netsuite.netsuite_service import pipeline_service
from tasks.tasks_service import TASKS, tasks_service
from main import main

TIME_RANGE = [
    # ("auto", (None, None)),
    ("manual", ("2018-01-01", "2019-01-01")),
    # ("dev", (date.today().isoformat(), (date.today() + timedelta(days=1)).isoformat())),
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
    def service(self, pipeline, start, end):
        res = pipeline_service(pipeline)(start, end)
        print(res)
        assert res["output_rows"] >= 0

    def controller(self, pipeline, start, end):
        data = {
            "table": pipeline.name,
            "start": start,
            "end": end,
        }
        res = main(Mock(get_json=Mock(return_value=data), args=data))
        print(res)
        assert res["output_rows"] >= 0

    @pytest.fixture(
        params=[
            service,
            # controller,
        ],
        ids=[
            "service",
            # "controller",
        ],
    )
    def mode(self, request):
        return request.param

    @pytest.mark.parametrize(**parameterize(static_pipelines))
    def test_static_pipeline(self, mode, pipeline):
        return mode(self, pipeline, None, None)

    @pytest.mark.parametrize(**parameterize(time_dynamic_pipelines))
    @pytest.mark.parametrize(
        "timerange",
        [tr[1] for tr in TIME_RANGE],
        ids=[tr[0] for tr in TIME_RANGE],
    )
    def test_time_dynamic_pipeline(self, mode, pipeline, timerange):
        return mode(self, pipeline, timerange[0], timerange[1])

    @pytest.mark.parametrize(**parameterize(id_dynamic_pipelines))
    @pytest.mark.parametrize(
        "idrange",
        [ir[1] for ir in ID_RANGE],
        ids=[ir[0] for ir in ID_RANGE],
    )
    def test_id_dynamic_pipeline(self, mode, pipeline, idrange):
        return mode(self, pipeline, idrange[0], idrange[1])


class TestTasks:
    @pytest.mark.parametrize(
        "task",
        TASKS.keys(),
        ids=TASKS.keys(),
    )
    def test_service(self, task):
        res = tasks_service({"task": task})
        assert res["tasks"] > 0
