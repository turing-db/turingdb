from turingapi.proto import APIService_pb2
import grpc


class TuringError(Exception):
    def __init__(self, details: str, code: grpc.StatusCode, err_type: str):
        self._details = details 
        self._code = code
        self._type = "Error" + err_type

    @property
    def details(self) -> str:
        return self._details

    @property
    def code(self):
        return self._code

    @property
    def type(self) -> str:
        return self._type


class Request:
    def __init__(self, turing, rpc_method: str, **kwargs):
        turing_error = None

        try:
            method = getattr(turing.stub, rpc_method)
            if len(kwargs) != 0:
                request = getattr(APIService_pb2, rpc_method + "Request")(**kwargs)
                self.data = method(request)
            else:
                request = getattr(APIService_pb2, rpc_method + "Request")()
                self.data = method(request)

        except grpc.RpcError as err:
            turing_error = TuringError(err.details(), err.code(), rpc_method)

        if turing_error is not None:
            raise turing_error

