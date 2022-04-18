from typing import Callable, Any, Optional
from dataclasses import dataclass, field

from jaydebeapi import Connection


@dataclass
class Key:
    id_key: list[str]
    cursor_key: list[str]
    rank_key: list[str] = field(default_factory=list)
    cursor_rank_key: list[str] = field(default_factory=list)
    cursor_rn_key: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.rank_key = self.rank_key if self.rank_key else self.id_key
        self.cursor_rank_key = (
            self.cursor_rank_key if self.cursor_rank_key else self.cursor_key
        )
        self.cursor_rn_key = (
            self.cursor_rn_key if self.cursor_rn_key else self.cursor_key
        )


@dataclass
class Pipeline:
    name: str
    schema: list[dict[str, Any]]
    conn_fn: Callable[[], Connection]
    query_fn: Callable[[tuple[Optional[str], Optional[str]]], str]
    param_fn: Callable[
        [Any, Any],
        Callable[[tuple[str, str]], tuple[Optional[str], Optional[str]]],
    ] = lambda *args: lambda _: (None, None)
    key: Optional[Key] = None
    load_callback_fn: Callable[
        [str, Any],
        Callable[[int], int],
    ] = lambda *args: lambda x: x
