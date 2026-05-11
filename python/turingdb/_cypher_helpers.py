from __future__ import annotations

from typing import Optional

from pandas import DataFrame

from .exceptions import TuringDBException


class CypherHelpersMixin:
    """Pure-Cypher operations shared across every TuringDB-style class.

    Anything implementable as ``self.query(<cypher>)`` lives here so the
    network clients (HTTP / binary) and the in-process engine present the
    same convenience API without duplicated bodies. Implementing classes
    must provide ``query``, ``set_change``, ``current_change``, and
    ``current_commit``.
    """

    def list_loaded_graphs(self) -> list[str]:
        df = self.query("LIST GRAPH")
        if df.empty:
            return []
        return df["graphName"].tolist()

    def is_graph_loaded(self) -> bool:
        return self.current_graph in self.list_loaded_graphs()

    def load_graph(self, graph_name: str, raise_if_loaded: bool = True) -> Optional[DataFrame]:
        if graph_name in self.list_loaded_graphs():
            if raise_if_loaded:
                raise TuringDBException("GRAPH_ALREADY_EXISTS")
            return None
        return self.query(f"LOAD GRAPH {graph_name}")

    def create_graph(self, graph_name: str) -> DataFrame:
        return self.query(f"CREATE GRAPH {graph_name}")

    def new_change(self) -> int:
        if self.current_change != "main":
            raise TuringDBException("Cannot create a new change while working on one")
        if self.current_commit != "HEAD":
            raise TuringDBException("Cannot create a new change while working on a commit")
        res = self.query("CHANGE NEW")
        change_id = int(res.loc[0, "changeID"])
        self.set_change(change_id)
        return change_id
