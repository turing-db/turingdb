from __future__ import annotations

from typing import Literal, Optional, Union

from pandas import DataFrame

from .binary_client import BinaryClient
from .embedded_client import EmbeddedClient
from .exceptions import TuringDBException


ClientType = Literal["native", "embedded"]

_UNSET = object()


class TuringDB:
    """Unified TuringDB client.

    Pick the backend with ``type``:
      - ``"native"`` (default): binary protocol to a daemon. Uses ``host`` and ``port``.
      - ``"embedded"``: in-process engine. Uses ``data_dir``.

    For backend-specific functionality reach the underlying impl via
    ``client.impl``.
    """

    def __init__(
        self,
        type: ClientType = "native",
        host=_UNSET,
        data_dir=_UNSET,
        port: Optional[Union[int, str]] = None,
        token: Optional[str] = None,
    ):
        if type == "embedded":
            if host is not _UNSET:
                raise TuringDBException(
                    "host is not valid for type='embedded'; pass data_dir instead"
                )
            if token is not None:
                raise TuringDBException(
                    "token is not valid for type='embedded'; embedded access is in-process and unauthenticated"
                )
            resolved_data_dir = None if data_dir is _UNSET else data_dir
            self._impl: Union[BinaryClient, EmbeddedClient] = EmbeddedClient(
                data_dir=resolved_data_dir
            )
        elif type == "native":
            if data_dir is not _UNSET:
                raise TuringDBException(
                    "data_dir is not valid for type='native'; pass host instead"
                )
            resolved_host = "localhost" if host is _UNSET else host
            if "://" in resolved_host:
                raise TuringDBException(
                    f"host must be a host name or IP address, not a URL: {resolved_host!r}; "
                    "pass the port with port="
                )
            resolved_port = 6666 if port is None else port
            self._impl = BinaryClient(host=resolved_host, port=resolved_port, token=token)
        else:
            raise TuringDBException(
                f"Unknown type: {type!r} (expected 'native' or 'embedded')"
            )

        self._type: ClientType = type

    @property
    def type(self) -> ClientType:
        return self._type

    @property
    def impl(self) -> Union[BinaryClient, EmbeddedClient]:
        return self._impl

    def query(self, query: str, query_parameters: Optional[dict] = None) -> DataFrame:
        return self._impl.query(query, query_parameters)

    def query_raw(self, query: str, query_parameters: Optional[dict] = None) -> dict:
        """Run the query and return the columns without building a DataFrame.

        The result is ``{"data": {column: values}, "dtypes": {column: type name},
        "time": milliseconds}``. Each ``values`` is a numpy array, or a list for
        strings, nullable columns, lists and maps.
        """
        return self._impl.query_raw(query, query_parameters)

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
