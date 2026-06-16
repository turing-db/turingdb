from __future__ import annotations

import os
import urllib.parse
from typing import Literal, Optional, Union

from pandas import DataFrame

from .binary_client import BinaryClient
from .embedded_client import EmbeddedClient
from .exceptions import TuringDBException
from .http_client import HTTPClient


ClientType = Literal["json", "native", "embedded"]

_UNSET = object()


class TuringDB:
    """Unified TuringDB client.

    Pick the backend with ``type``:
      - ``"json"`` (default): REST/HTTP to a daemon. Uses ``host``.
      - ``"native"``: binary protocol to a daemon. Uses ``host``.
      - ``"embedded"``: in-process engine. Uses ``data_dir``.

    The ``TURINGDB_TYPE`` environment variable picks the default when
    ``type`` is omitted (used by the regress runner to exercise multiple
    backends without modifying every test).

    For backend-specific functionality (e.g. ``list_available_graphs`` on
    the JSON client) reach the underlying impl via ``client.impl``.
    """

    def __init__(
        self,
        type: Optional[ClientType] = None,
        host=_UNSET,
        data_dir=_UNSET,
        port: Optional[Union[int, str]] = None,
        token: Optional[str] = None,
    ):
        resolved_type = type if type is not None else _default_type()

        if resolved_type == "embedded":
            if host is not _UNSET:
                raise TuringDBException(
                    "host is not valid for type='embedded'; pass data_dir instead"
                )
            if token is not None:
                raise TuringDBException(
                    "token is not valid for type='embedded'; embedded access is in-process and unauthenticated"
                )
            resolved_data_dir = None if data_dir is _UNSET else data_dir
            self._impl: Union[HTTPClient, BinaryClient, EmbeddedClient] = EmbeddedClient(
                data_dir=resolved_data_dir
            )
        elif resolved_type == "json":
            if data_dir is not _UNSET:
                raise TuringDBException(
                    "data_dir is not valid for type='json'; pass host instead"
                )
            resolved_host = "http://localhost:6666" if host is _UNSET else host
            self._impl = HTTPClient(host=resolved_host, token=token)
        elif resolved_type == "native":
            if data_dir is not _UNSET:
                raise TuringDBException(
                    "data_dir is not valid for type='native'; pass host instead"
                )
            resolved_host = "http://localhost:6666" if host is _UNSET else host
            binary_host, binary_port = _split_host_port(resolved_host, port)
            self._impl = BinaryClient(host=binary_host, port=binary_port, token=token)
        else:
            raise TuringDBException(
                f"Unknown type: {resolved_type!r} (expected 'json', 'native', or 'embedded')"
            )

        self._type: ClientType = resolved_type

    @property
    def type(self) -> ClientType:
        return self._type

    @property
    def impl(self) -> Union[HTTPClient, BinaryClient, EmbeddedClient]:
        return self._impl

    def query(self, query: str) -> DataFrame:
        return self._impl.query(query)

    def query_raw(self, query: str) -> dict:
        return self._impl.query_raw(query)

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

    def list_available_graphs(self) -> list[str]:
        return self._impl.list_available_graphs()

    def is_graph_loaded(self) -> bool:
        return self._impl.is_graph_loaded()

    def load_graph(self, graph_name: str, raise_if_loaded: bool = True):
        return self._impl.load_graph(graph_name, raise_if_loaded=raise_if_loaded)

    def reconnect(self) -> None:
        self._impl.reconnect()

    def try_reach(self, timeout: int = 5) -> None:
        self._impl.try_reach(timeout)

    def warmup(self, timeout: int = 5) -> None:
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


def _default_type() -> ClientType:
    env = os.environ.get("TURINGDB_TYPE", "").strip().lower()
    if env in ("json", "native", "embedded"):
        return env  # type: ignore[return-value]
    if env:
        raise TuringDBException(
            f"Invalid TURINGDB_TYPE={env!r}; expected 'json', 'native', or 'embedded'"
        )
    return "json"


def _split_host_port(host: str, port: Optional[Union[int, str]]) -> tuple[str, str]:
    if "://" in host:
        parsed = urllib.parse.urlparse(host)
        resolved_host = parsed.hostname or "localhost"
        resolved_port = parsed.port if parsed.port is not None else port
    else:
        resolved_host = host
        resolved_port = port
    return resolved_host, str(resolved_port if resolved_port is not None else 6666)
