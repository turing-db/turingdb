from typing import Protocol

from pandas import DataFrame


class QueryProtocol(Protocol):
    def query(self, query: str) -> DataFrame: ...
