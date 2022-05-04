from netsuite.pipeline import pipelines
from netsuite.netsuite_service import pipeline_service


def netsuite_controller(body: dict[str, str]):
    return pipeline_service(pipelines[body.get("table")])(
        body.get("start"),
        body.get("end"),
    )
