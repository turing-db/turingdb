from __future__ import annotations
from collections.abc import Iterable, Mapping
from turingapi.proto.APIService_pb2_grpc import APIServiceStub, APIService
from turingapi.requests import Response
from turingapi.core import Database
import grpc

class Turing:
    def __init__(self, connection_str):
        self.channel = grpc.insecure_channel(connection_str, options=(('grpc.enable_http_proxy', 0),))
        self.stub = APIServiceStub(self.channel)

    @property
    def running(self) -> bool:
        res = Response.get(self, "GetStatus")
        if res.failed():
            return False

        return True

    def list_available_databases(self) -> Response[Iterable[str]]:
        res = Response.get(self, "ListAvailableDB")
        if res.failed():
            return res

        res.data = res.data.db_names
        return res

    def list_loaded_databases(self) -> Response[Mapping[str, Database]]:
        res = Response.get(self, "ListLoadedDB")
        if res.failed():
            return res

        res.data = { name: Database(self, db) for name, db in res.data.dbs.items() }
        return res

    def load_db(self, db_name: str) -> Response[Database]:
        res = Response.get(self, "LoadDB", db_name=db_name)
        if res.failed():
            return res

        res.data = Database(self, res.data.db)
        return res

    def get_db(self, db_name: str) -> Response[Database]:
        res = self.load_db(db_name=db_name)
        if res.failed() and res.status_code == grpc.StatusCode.NOT_FOUND:
            return res

        res = self.list_loaded_databases()
        if res.failed():
            return res

        res.data = res.data[db_name]
        return res

    def create_db(self, db_name: str) -> Response[Database]:
        res = Response.get(self, "CreateDB", db_name=db_name)
        if res.failed():
            return res

        res.data = Database(self, res.data.db)
        return res
