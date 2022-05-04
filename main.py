from typing import Any

from netsuite.netsuite_controller import netsuite_controller
from tasks.tasks_service import tasks_service


def main(request):
    data: dict[str, Any] = request.get_json()
    print(data)

    if "task" in data and "table" not in data:
        response = tasks_service(data)
    else:
        response = netsuite_controller(data)

    print(response)
    return response
