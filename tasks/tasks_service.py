from netsuite.pipeline import (
    static_pipelines,
    time_dynamic_pipelines,
    id_dynamic_pipelines,
)
from tasks import cloud_tasks

TASKS = {
    "static": static_pipelines,
    "dynamic": time_dynamic_pipelines | id_dynamic_pipelines,
}


def tasks_service(body: dict[str, str]):
    return {
        "tasks": cloud_tasks.create_tasks(
            [
                {
                    "table": table.name,
                    "start": body.get("start"),
                    "end": body.get("end"),
                }
                for table in TASKS[body["task"]].values()
            ],
            lambda x: x["table"],
        )
    }
