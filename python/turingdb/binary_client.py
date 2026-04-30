from __future__ import annotations

import time
from typing import Literal, Optional

from pandas import DataFrame

from .exceptions import TuringDBException
from .s3 import S3Client


class BinaryClient:
    """Pythonic wrapper over the nanobind TuringDB Binary Protocol client.

    Returns query results as pandas.DataFrame. The underlying protocol streams
    one or more chunks per query; chunks are buffered into a single dataframe
    on the C++ side and returned as a dict of numpy arrays / lists.
    """

    def __init__(self, host: str = "localhost", port: int | str = 6666):
        # Deferred import: the compiled .so may be absent on systems that only
        # use the HTTP transport, in which case the facade should still load.
        from ._binary import TuringProtoClient as _Client

        self._inner = _Client(host, str(port))
        self._graph: str = "default"
        self._change: Optional[str] = None
        self._commit: Optional[str] = None
        self._s3_client: Optional[S3Client] = None
        self._query_exec_time: Optional[float] = None
        self._total_exec_time: Optional[float] = None
        self._t0: float = 0
        self._t1: float = 0
        self._call(self._inner.connect)
        self._call(self._inner.set_graph_name, self._graph)

    @staticmethod
    def _call(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except RuntimeError as e:
            raise TuringDBException(str(e)) from e

    # `timeout` is ignored: the binary protocol's TuringClient has no socket-timeout API yet.
    def try_reach(self, timeout: int = 5) -> None:
        self.query("LIST GRAPH")

    # Same caveat as try_reach: `timeout` is ignored.
    def warmup(self, timeout: int = 5) -> None:
        self.query("LIST GRAPH")

    def set_graph(self, name: str) -> None:
        self._call(self._inner.set_graph_name, name)
        self._graph = name

    def get_graph(self) -> str:
        return self._graph

    def set_change(self, change: int | str) -> None:
        if isinstance(change, str) and change.lower() in ("main", "head"):
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
        if commit.lower() == "head":
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

    def create_graph(self, graph_name: str) -> DataFrame:
        return self.query(f"create graph {graph_name}")

    def list_loaded_graphs(self) -> list[str]:
        df = self.query("LIST GRAPH")
        if df.empty:
            return []
        return df["graphName"].tolist()

    def list_available_graphs(self) -> list[str]:
        raise TuringDBException(
            "list_available_graphs is not supported by the binary protocol; "
            "use the HTTP TuringDB client to enumerate on-disk graphs"
        )

    def is_graph_loaded(self) -> bool:
        return self._graph in self.list_loaded_graphs()

    def load_graph(self, graph_name: str, raise_if_loaded: bool = True) -> Optional[DataFrame]:
        if graph_name in self.list_loaded_graphs():
            if raise_if_loaded:
                raise TuringDBException("GRAPH_ALREADY_EXISTS")
            return None
        return self.query(f"LOAD GRAPH {graph_name}")

    def new_change(self) -> int:
        if self._change is not None:
            raise TuringDBException("Cannot create a new change while working on one")
        if self._commit is not None:
            raise TuringDBException("Cannot create a new change while working on a commit")
        res = self.query("CHANGE NEW")
        change_id = int(res.loc[0, "changeID"])
        self.set_change(change_id)
        return change_id

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

    def get_query_exec_time(self) -> Optional[float]:
        return self._query_exec_time

    def get_total_exec_time(self) -> Optional[float]:
        return self._total_exec_time

    def s3_connect(
        self,
        bucket_name: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        use_scratch: bool = True,
    ):
        self._s3_client = S3Client(
            bucket_name, access_key, secret_key, region, use_scratch
        )
        self._s3_client.connect(self)

    def transfer(self, src: str, dst: str):
        if self._s3_client is None:
            raise TuringDBException("S3 client is not connected")

        self._s3_client.transfer(src, dst)

    @property
    def current_graph(self) -> str:
        return self._graph

    @property
    def current_commit(self) -> str:
        return self._commit or "HEAD"

    @property
    def current_change(self) -> str:
        return self._change or "main"
