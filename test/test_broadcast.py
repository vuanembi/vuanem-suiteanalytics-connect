import pytest

from .utils import process


@pytest.mark.parametrize(
    "broadcast",
    [
        {
            "broadcast": "standard",
        },
        {
            "broadcast": "incre",
        },
    ],
    ids=[
        "standard",
        "auto",
    ],
)
def test_broadcast(broadcast):
    res = process(broadcast)
    assert res["message_sent"] > 0
