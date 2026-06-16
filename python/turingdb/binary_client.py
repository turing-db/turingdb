from __future__ import annotations

import time
from typing import Literal, Optional

from pandas import DataFrame

from ._cypher_helpers import CypherHelpersMixin
from .exceptions import TuringDBException
from .s3 import S3Client


class BinaryClient(CypherHelpersMixin):
    """Pythonic wrapper over the nanobind TuringDB Binary Protocol client.

    The binary protocol runs Turing's proto packets inside HTTP/1.1 chunked
    transfer encoding — every proto packet is one HTTP chunk.

    Returns query results as pandas.DataFrame. The underlying protocol streams
    one or more chunks per query; chunks are buffered into a single dataframe
    on the C++ side and returned as a dict of numpy arrays / lists.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int | str = 6666,
        token: Optional[str] = None,
    ):
        # Deferred import: the compiled .so may be absent on systems that
        # only use the HTTP transport, in which case the facade should still
        # load.
        import os

        from ._binary import TuringProtoClient as _Client

        self._inner = _Client(host, str(port))

        # Authentication token: an explicit argument wins (including an empty
        # string, which deliberately suppresses auth), else the
        # TURINGDB_AUTH_TOKEN environment variable. Sent as a bearer token in
        # the Authorization header on every request. Left unset when neither is
        # present (the server then only accepts it when launched without
        # -auth-on).
        token = token if token is not None else os.environ.get("TURINGDB_AUTH_TOKEN")
        if token:
            self._inner.set_auth_token(token)
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

    def reconnect(self) -> None:
        """Drop the current TCP socket and open a new one.

        Session state (current graph, change, commit) is reset — the new
        daemon has no memory of our previous context. Callers should re-call
        ``set_graph`` / ``load_graph`` / ``set_change`` etc. to re-establish
        whatever they need.
        """
        if self._inner.is_connected():
            try:
                self._inner.disconnect()
            except RuntimeError:
                pass
        self._call(self._inner.connect)
        self._graph = "default"
        self._change = None
        self._commit = None
        self._call(self._inner.set_graph_name, self._graph)
        self._call(self._inner.clear_change_id)
        self._call(self._inner.clear_commit_hash)

    # `timeout` is ignored: the binary protocol client has no socket-timeout API yet.
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

    def list_available_graphs(self) -> list[str]:
        raise TuringDBException(
            "list_available_graphs is not supported by the binary protocol; "
            "use the HTTP TuringDB client to enumerate on-disk graphs"
        )

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
