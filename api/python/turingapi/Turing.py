from __future__ import annotations
from collections.abc import Iterable, Mapping
from turingapi.proto.APIService_pb2_grpc import APIServiceStub
from turingapi.core import Database
from turingapi.Request import Request, TuringError
import grpc


class Turing:
    def __init__(self, connection_str):
        self.channel = grpc.insecure_channel(
            connection_str, options=(("grpc.enable_http_proxy", 0),)
        )
        self.stub = APIServiceStub(self.channel)

    @property
    def running(self) -> bool:
        try:
            Request(self, "GetStatus")
            return True
        except:
            return False

    def list_available_databases(self) -> Iterable[str]:
        res = Request(self, "ListAvailableDB")
        return res.data.db_names

    def list_loaded_databases(self) -> Mapping[str, Database]:
        res = Request(self, "ListLoadedDB")
        return {name: Database(self, db) for name, db in res.data.dbs.items()}

    def load_db(self, db_name: str) -> Database:
        res = Request(self, "LoadDB", db_name=db_name)
        return Database(self, res.data.db)

    def get_db(self, db_name: str) -> Database:
        try:
            return self.load_db(db_name=db_name)
        except TuringError:
            res = self.list_loaded_databases()
            return res[db_name]

    def create_db(self, db_name: str) -> Database:
        res = Request(self, "CreateDB", db_name=db_name)
        return Database(self, res.data.db)
