from __future__ import annotations

import time
from typing import Literal, Optional

from pandas import DataFrame

from ._cypher_helpers import CypherHelpersMixin
from .exceptions import TuringDBException


class EmbeddedClient(CypherHelpersMixin):
    """In-process TuringDB engine.

    Runs the database in the caller's process — no daemon, no sockets.
    State (graph, change, commit) accumulates on the client between
    ``query()`` calls, mirroring the network client shape so callers can
    move between transports without rewriting their code.

    If ``data_dir`` is omitted the engine uses the C++ default of
    ``$HOME/.turing``.
    """

    def __init__(self, data_dir: Optional[str] = None):
        from ._embedded import PyTuringEmbedded as _Engine

        self._inner = _Engine() if data_dir is None else _Engine(data_dir)
        self._graph: str = "default"
        self._change: Optional[str] = None
        self._commit: Optional[str] = None
        self._query_exec_time: Optional[float] = None
        self._total_exec_time: Optional[float] = None
        self._t0: float = 0
        self._t1: float = 0
        self._call(self._inner.set_graph_name, self._graph)

    @staticmethod
    def _call(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as e:
            raise TuringDBException(str(e)) from e

    def reconnect(self) -> None:
        pass

    def try_reach(self, timeout: int = 5) -> None:
        pass

    def warmup(self, timeout: int = 5) -> None:
        pass

    def set_graph(self, name: str) -> None:
        self._call(self._inner.set_graph_name, name)
        self._graph = name

    def get_graph(self) -> str:
        return self._graph

    def set_change(self, change: int | str) -> None:
        if change == "main" or change == "head":
            self._call(self._inner.clear_change_id)
            self._change = None
            return
        if isinstance(change, str):
            change_int = int(change, 16)
            change_str = change
        else:
            change_int = change
            change_str = f"{change:x}"
        self._call(self._inner.set_change_id, change_int)
        self._change = change_str

    def set_commit(self, commit: str) -> None:
        if commit == "HEAD" or commit == "head":
            self._call(self._inner.clear_commit_hash)
            self._commit = None
            return
        self._call(self._inner.set_commit_hash, commit)
        self._commit = commit

    def checkout(self, change: int | Literal["main"] = "main", commit: str = "HEAD") -> None:
        self.set_change(change)
        if commit != "HEAD":
            self.query(f"LOAD COMMIT '{commit}'")
        self.set_commit(commit)

    def query(self, cypher: str) -> DataFrame:
        import pandas as pd

        from .protocol import DTYPE_MAP

        raw = self._timed_query(cypher)
        data = raw["data"]
        dtypes = raw["dtypes"]
        return pd.DataFrame(
            {
                col: pd.Series(values, dtype=DTYPE_MAP.get(dtypes[col], "object"))
                for col, values in data.items()
            }
        )

    def query_raw(self, cypher: str) -> dict:
        return self._timed_query(cypher)

    def _timed_query(self, cypher: str) -> dict:
        self._query_exec_time = None
        self._total_exec_time = None
        self._t0 = time.time()
        result = self._call(self._inner.query, cypher)
        self._t1 = time.time()
        self._total_exec_time = (self._t1 - self._t0) * 1000
        self._query_exec_time = result.get("time")
        return result

    def list_available_graphs(self) -> list[str]:
        raise TuringDBException(
            "list_available_graphs is not supported by the embedded engine; "
            "use type='json' to enumerate on-disk graphs"
        )

    def s3_connect(self, *args, **kwargs) -> None:
        raise TuringDBException(
            "S3 operations are not supported by the embedded engine; "
            "use type='json' or type='native' to connect to a daemon"
        )

    def transfer(self, *args, **kwargs) -> None:
        raise TuringDBException(
            "S3 operations are not supported by the embedded engine; "
            "use type='json' or type='native' to connect to a daemon"
        )

    def get_query_exec_time(self) -> Optional[float]:
        return self._query_exec_time

    def get_total_exec_time(self) -> Optional[float]:
        return self._total_exec_time

    @property
    def current_graph(self) -> str:
        return self._graph

    @property
    def current_commit(self) -> str:
        return self._commit or "HEAD"

    @property
    def current_change(self) -> str:
        return self._change or "main"
