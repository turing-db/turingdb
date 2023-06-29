import grpc

from turingapi.proto.APIService_pb2_grpc import APIService, APIServiceStub
import turingapi.proto.APIService_pb2 as APIService_pb2

class Turing:
    def __init__(self, connection_str):
        self.channel = grpc.insecure_channel(connection_str)
        self.stub = APIServiceStub(self.channel)


    def get_status(self):
        response = self.stub.GetStatus(APIService_pb2.StatusRequest())
        return response.msg
