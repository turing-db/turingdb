#include "APIServiceImpl.h"

grpc::Status APIServiceImpl::GetStatus(grpc::ServerContext* ctxt,
                                       const StatusRequest& request,
                                       StatusReply* reply) {
    reply->set_msg("OK");
    return grpc::Status::OK;
}
