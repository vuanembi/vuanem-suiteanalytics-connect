from unittest.mock import Mock

import pytest

from main import main
from .utils import encode_data


@pytest.mark.parametrize(
    "broadcast",
    [
        "standard",
        "incre",
    ],
)
def test_broadcast(broadcast):
    data = {
        "broadcast": broadcast,
    }
    message = encode_data(data)
    req = Mock(get_json=Mock(return_value=message), args=message)
    res = main(req)
    assert res["message_sent"] > 0
