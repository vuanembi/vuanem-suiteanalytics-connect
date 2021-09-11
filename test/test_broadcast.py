import pytest

from .utils import process


@pytest.mark.parametrize(
    "broadcast",
    [
        "standard",
        "incre",
    ],
)
def test_broadcast(broadcast):
    res = process(broadcast)
    assert res["message_sent"] > 0
