from __future__ import annotations
import turingapi.proto.APIService_pb2 as APIService_pb2
import grpc

from typing import TypeVar, Generic
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from turingapi.core import Turing

T = TypeVar('T')

class Response(Generic[T]):
    def __init__(self):
        self.data: T = None
        self.error_type = "No error"
        self.error_details = "OK"
        self.status_code = grpc.StatusCode.OK

    @staticmethod
    def get(turing: Turing, rpc_method: str, **kwargs) -> Response:
        response = Response()

        try:
            method = getattr(turing.stub, rpc_method)
            if len(kwargs) != 0:
                request = getattr(APIService_pb2, rpc_method + "Request")(**kwargs)
            else:
                request = getattr(APIService_pb2, rpc_method + "Request")()

            response.data = method(request)

        except grpc.RpcError as e:
            response.error_type = f"{rpc_method} error"
            response.error_details = e.details()
            response.status_code = e.code()

        return response

    def __bool__(self) -> bool:
        return self.status_code == grpc.StatusCode.OK

    def failed(self) -> bool:
        return self.status_code != grpc.StatusCode.OK

    def report_error(self) -> None:
        print(f"An error occured: {self.error_type}")
        print(f"Error code: {self.status_code}")
        print(f"Error details: {self.error_details}")


