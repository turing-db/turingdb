#pragma once

#include "APIService.grpc.pb.h"

class APIServiceImpl : public APIService::Service {
public:
    grpc::Status GetStatus(grpc::ServerContext* ctxt,
                           const StatusRequest& request,
                           StatusReply* reply);
};
