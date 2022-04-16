from typing import Callable, Any, Optional, Protocol
from dataclasses import dataclass

from jaydebeapi import Connection


class LoadCallback(Protocol):
    def __call__(self, *args) -> Callable[[int], int]:
        pass


@dataclass
class Key:
    id_key: list[str]
    rank_key: list[str]
    cursor_key: list[str]
    cursor_rank_key: list[str]
    cursor_rn_key: list[str]


@dataclass
class Pipeline:
    name: str
    schema: list[dict[str, Any]]
    conn_fn: Callable[[], Connection]
    query_fn: Callable[[tuple[Optional[str], Optional[str]]], str]
    param_fn: Callable[
        [Any, Any],
        Callable[[tuple[str, str]], tuple[str, str]],
    ] = lambda *args: lambda _: (None, None)
    key: Optional[Key] = None
    load_callback_fn: LoadCallback = lambda *args: lambda x: x
