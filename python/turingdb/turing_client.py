from __future__ import annotations

import os
import urllib.parse
from typing import Literal, Optional, Union

from pandas import DataFrame

from .exceptions import TuringDBException
from .http_client import HTTPClient
from .binary_client import BinaryClient


Transport = Literal["http", "binary"]


class TuringClient:
    """Unified TuringDB network client.

    Pass ``transport="http"`` (default) for the REST/HTTP client or
    ``transport="binary"`` for the binary-protocol client. If ``transport``
    is left unset the ``TURINGDB_TRANSPORT`` environment variable picks the
    default (used by the regress runner to exercise both transports without
    modifying every test).

    The facade exposes the methods that behave identically across both
    transports. For transport-specific functionality (e.g.
    ``list_available_graphs`` on HTTP, or the raw column-dict response from
    the binary protocol) reach the underlying client via ``client.http`` or
    ``client.binary``.

    For an embedded engine that runs the database in this process rather
    than talking to a daemon, use :class:`turingdb.TuringDB` instead.
    """

    def __init__(
        self,
        host: str = "http://localhost:6666",
        transport: Optional[Transport] = None,
        port: Optional[Union[int, str]] = None,
    ):
        resolved = transport if transport is not None else _default_transport()
        if resolved == "http":
            self._impl: Union[HTTPClient, BinaryClient] = HTTPClient(host=host)
        elif resolved == "binary":
            binary_host, binary_port = _split_host_port(host, port)
            self._impl = BinaryClient(host=binary_host, port=binary_port)
        else:
            raise TuringDBException(f"Unknown transport: {resolved!r}")

        self._transport: Transport = resolved

    @property
    def transport(self) -> Transport:
        return self._transport

    @property
    def http(self) -> Optional[HTTPClient]:
        return self._impl if isinstance(self._impl, HTTPClient) else None

    @property
    def binary(self) -> Optional[BinaryClient]:
        return self._impl if isinstance(self._impl, BinaryClient) else None

    def query(self, query: str) -> DataFrame:
        return self._impl.query(query)

    def set_graph(self, graph_name: str) -> None:
        self._impl.set_graph(graph_name)

    def get_graph(self) -> str:
        return self._impl.get_graph()

    def set_change(self, change: int | str) -> None:
        self._impl.set_change(change)

    def set_commit(self, commit: str) -> None:
        self._impl.set_commit(commit)

    def checkout(self, change: int | Literal["main"] = "main", commit: str = "HEAD") -> None:
        self._impl.checkout(change, commit)

    def new_change(self) -> int:
        return self._impl.new_change()

    def create_graph(self, graph_name: str):
        return self._impl.create_graph(graph_name)

    def list_loaded_graphs(self) -> list[str]:
        return self._impl.list_loaded_graphs()

    def is_graph_loaded(self) -> bool:
        return self._impl.is_graph_loaded()

    def load_graph(self, graph_name: str, raise_if_loaded: bool = True):
        return self._impl.load_graph(graph_name, raise_if_loaded=raise_if_loaded)

    def reconnect(self) -> None:
        """Refresh the underlying transport.

        On the binary transport this drops the current TCP socket and opens
        a new one, re-applying session state (graph, change, commit). Useful
        after a daemon restart. No-op on HTTP since httpx connects per request.
        """
        self._impl.reconnect()

    def try_reach(self, timeout: int = 5) -> None:
        # The ``timeout`` argument is honored on HTTP; the binary transport ignores
        # it because the binary protocol's TuringClient has no socket-timeout API yet.
        self._impl.try_reach(timeout)

    def warmup(self, timeout: int = 5) -> None:
        # Same caveat as try_reach about ``timeout``.
        self._impl.warmup(timeout)

    def s3_connect(
        self,
        bucket_name: str,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: Optional[str] = None,
        use_scratch: bool = True,
    ):
        self._impl.s3_connect(bucket_name, access_key, secret_key, region, use_scratch)

    def transfer(self, src: str, dst: str) -> None:
        self._impl.transfer(src, dst)

    def get_query_exec_time(self) -> Optional[float]:
        return self._impl.get_query_exec_time()

    def get_total_exec_time(self) -> Optional[float]:
        return self._impl.get_total_exec_time()

    @property
    def current_graph(self) -> str:
        return self._impl.current_graph

    @property
    def current_commit(self) -> str:
        return self._impl.current_commit

    @property
    def current_change(self) -> str:
        return self._impl.current_change


def _default_transport() -> Transport:
    env = os.environ.get("TURINGDB_TRANSPORT", "").strip().lower()
    if env in ("http", "binary"):
        return env  # type: ignore[return-value]
    if env:
        raise TuringDBException(
            f"Invalid TURINGDB_TRANSPORT={env!r}; expected 'http' or 'binary'"
        )
    return "http"


def _split_host_port(host: str, port: Optional[Union[int, str]]) -> tuple[str, str]:
    if "://" in host:
        parsed = urllib.parse.urlparse(host)
        resolved_host = parsed.hostname or "localhost"
        resolved_port = parsed.port if parsed.port is not None else port
    else:
        resolved_host = host
        resolved_port = port
    return resolved_host, str(resolved_port if resolved_port is not None else 6666)
