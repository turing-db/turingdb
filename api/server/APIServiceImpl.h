#pragma once

#include "APIService.grpc.pb.h"

namespace db {
class DB;
}

class RPCServerConfig;

class APIServiceImpl : public APIService::Service {
public:
    APIServiceImpl(const RPCServerConfig& config);
    ~APIServiceImpl() override;

    grpc::Status GetStatus(grpc::ServerContext* ctxt,
                           const GetStatusRequest* request,
                           GetStatusReply* reply) override;

    grpc::Status ListAvailableDB(grpc::ServerContext* ctxt,
                                 const ListAvailableDBRequest* request,
                                 ListAvailableDBReply* reply) override;

    grpc::Status ListLoadedDB(grpc::ServerContext* ctxt,
                              const ListLoadedDBRequest* request,
                              ListLoadedDBReply* reply) override;

    grpc::Status LoadDB(grpc::ServerContext* ctxt,
                        const LoadDBRequest* request,
                        LoadDBReply* reply) override;

    grpc::Status UnloadDB(grpc::ServerContext* ctxt,
                          const UnloadDBRequest* request,
                          UnloadDBReply* reply) override;

    grpc::Status CreateDB(grpc::ServerContext* ctxt,
                          const CreateDBRequest* request,
                          CreateDBReply* reply) override;

    grpc::Status DumpDB(grpc::ServerContext* ctxt,
                        const DumpDBRequest* request,
                        DumpDBReply* reply) override;

    grpc::Status ListNodeTypes(grpc::ServerContext* ctxt,
                               const ListNodeTypesRequest* request,
                               ListNodeTypesReply* reply) override;

    grpc::Status ListNodeTypesByID(grpc::ServerContext* ctxt,
                                   const ListNodeTypesByIDRequest* request,
                                   ListNodeTypesByIDReply* reply) override;

    grpc::Status ListEdgeTypes(grpc::ServerContext* ctxt,
                               const ListEdgeTypesRequest* request,
                               ListEdgeTypesReply* reply) override;

    grpc::Status ListEdgeTypesByID(grpc::ServerContext* ctxt,
                                   const ListEdgeTypesByIDRequest* request,
                                   ListEdgeTypesByIDReply* reply) override;

    grpc::Status ListNetworks(grpc::ServerContext* ctxt,
                              const ListNetworksRequest* request,
                              ListNetworksReply* reply) override;

    grpc::Status ListNodes(grpc::ServerContext* ctxt,
                           const ListNodesRequest* request,
                           ListNodesReply* reply) override;

    grpc::Status ListNodesByID(grpc::ServerContext* ctxt,
                               const ListNodesByIDRequest* request,
                               ListNodesByIDReply* reply) override;

    grpc::Status ListEdges(grpc::ServerContext* ctxt,
                           const ListEdgesRequest* request,
                           ListEdgesReply* reply) override;

    grpc::Status ListEdgesByID(grpc::ServerContext* ctxt,
                               const ListEdgesByIDRequest* request,
                               ListEdgesByIDReply* reply) override;

    grpc::Status ListNodesFromNetwork(grpc::ServerContext* ctxt,
                                      const ListNodesFromNetworkRequest* request,
                                      ListNodesFromNetworkReply* reply) override;

    grpc::Status ListNodesByIDFromNetwork(grpc::ServerContext* ctxt,
                                          const ListNodesByIDFromNetworkRequest* request,
                                          ListNodesByIDFromNetworkReply* reply) override;

    grpc::Status ListEdgesFromNetwork(grpc::ServerContext* ctxt,
                                      const ListEdgesFromNetworkRequest* request,
                                      ListEdgesFromNetworkReply* reply) override;

    grpc::Status ListEdgesByIDFromNetwork(grpc::ServerContext* ctxt,
                                          const ListEdgesByIDFromNetworkRequest* request,
                                          ListEdgesByIDFromNetworkReply* reply) override;

    grpc::Status ListEntityProperties(grpc::ServerContext* ctxt,
                                      const ListEntityPropertiesRequest* request,
                                      ListEntityPropertiesReply* reply) override;

    grpc::Status CreateNodeType(grpc::ServerContext* ctxt,
                                const CreateNodeTypeRequest* request,
                                CreateNodeTypeReply* reply) override;

    grpc::Status CreateEdgeType(grpc::ServerContext* ctxt,
                                const CreateEdgeTypeRequest* request,
                                CreateEdgeTypeReply* reply) override;

    grpc::Status CreateNetwork(grpc::ServerContext* ctxt,
                               const CreateNetworkRequest* request,
                               CreateNetworkReply* reply) override;

    grpc::Status CreateNode(grpc::ServerContext* ctxt,
                            const CreateNodeRequest* request,
                            CreateNodeReply* reply) override;

    grpc::Status CreateEdge(grpc::ServerContext* ctxt,
                            const CreateEdgeRequest* request,
                            CreateEdgeReply* reply) override;

    grpc::Status AddEntityProperty(grpc::ServerContext* ctxt,
                                   const AddEntityPropertyRequest* request,
                                   AddEntityPropertyReply* reply) override;

    grpc::Status AddPropertyType(grpc::ServerContext* ctxt,
                                 const AddPropertyTypeRequest* request,
                                 AddPropertyTypeReply* reply) override;

    grpc::Status ListPropertyTypes(grpc::ServerContext* ctxt,
                                   const ListPropertyTypesRequest* request,
                                   ListPropertyTypesReply* reply) override;

private:
    const RPCServerConfig& _config;
    std::map<size_t, db::DB*> _databases;
    std::map<std::string, size_t> _dbNameMapping;
    size_t _nextAvailableId = 0;

    void listDiskDB(std::vector<std::string>& databaseNames);
    bool isDBValid(size_t id) const;
};
