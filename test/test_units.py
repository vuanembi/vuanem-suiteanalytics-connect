from datetime import date, timedelta

import pytest

from netsuite.pipeline import (
    static_pipelines,
    time_dynamic_pipelines,
    id_dynamic_pipelines,
)
from netsuite.netsuite_service import pipeline_service
from tasks.tasks_service import TASKS, tasks_service

TIME_RANGE = [
    # ("auto", (None, None)),
    # ("manual", ("2022-01-10", "2022-01-11")),
    ("dev", (date.today().isoformat(), (date.today() + timedelta(days=1)).isoformat())),
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

    @pytest.mark.parametrize(**parameterize(static_pipelines))
    def test_static_pipeline(self, pipeline):
        return self.service(pipeline, None, None)

    @pytest.mark.parametrize(**parameterize(time_dynamic_pipelines))
    @pytest.mark.parametrize(
        "timerange",
        [tr[1] for tr in TIME_RANGE],
        ids=[tr[0] for tr in TIME_RANGE],
    )
    def test_time_dynamic_pipeline(self, pipeline, timerange):
        return self.service(pipeline, timerange[0], timerange[1])

    @pytest.mark.parametrize(**parameterize(id_dynamic_pipelines))
    @pytest.mark.parametrize(
        "idrange",
        [ir[1] for ir in ID_RANGE],
        ids=[ir[0] for ir in ID_RANGE],
    )
    def test_id_dynamic_pipeline(self, pipeline, idrange):
        return self.service(pipeline, idrange[0], idrange[1])


class TestTasks:
    @pytest.mark.parametrize(
        "task",
        TASKS.keys(),
        ids=TASKS.keys(),
    )
    def test_service(self, task):
        res = tasks_service({"task": task})
        res
