#include "APIServiceImpl.h"

#include "BioAssert.h"
#include "DB.h"
#include "DBDumper.h"
#include "DBLoader.h"
#include "Edge.h"
#include "EdgeMap.h"
#include "EdgeType.h"
#include "FileUtils.h"
#include "Network.h"
#include "Node.h"
#include "NodeType.h"
#include "RPCServerConfig.h"
#include "Writeback.h"

#define MAX_ENTITY_COUNT 50000

static const grpc::Status invalidDBStatus {grpc::StatusCode::NOT_FOUND,
                                           "The database ID is invalid"};

APIServiceImpl::APIServiceImpl(const RPCServerConfig& config)
    : _config(config)
{
}

APIServiceImpl::~APIServiceImpl() {
    for (auto& [index, db] : _databases) {
        delete db;
    }
}

grpc::Status APIServiceImpl::GetStatus(grpc::ServerContext* ctxt,
                                       const GetStatusRequest* request,
                                       GetStatusReply* reply) {
    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListAvailableDB(grpc::ServerContext* ctxt,
                                             const ListAvailableDBRequest* request,
                                             ListAvailableDBReply* reply) {

    std::vector<std::string> diskDbNames;
    listDiskDB(diskDbNames);

    for (std::string& name : diskDbNames) {
        reply->add_db_names(std::move(name));
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListLoadedDB(grpc::ServerContext* ctxt,
                                          const ListLoadedDBRequest* request,
                                          ListLoadedDBReply* reply) {

    auto& dbs = *reply->mutable_dbs();
    for (const auto& [name, id] : _dbNameMapping) {
        auto& db = dbs[name];
        db.set_name(name);
        db.set_id(id);
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::LoadDB(grpc::ServerContext* ctxt,
                                    const LoadDBRequest* request,
                                    LoadDBReply* reply) {
    std::vector<std::string> diskDbNames;
    listDiskDB(diskDbNames);

    if (_dbNameMapping.find(request->db_name()) != _dbNameMapping.end()) {
        return grpc::Status {grpc::ALREADY_EXISTS,
                             "The database is already loaded"};
    }

    auto it = std::find(diskDbNames.cbegin(), diskDbNames.cend(), request->db_name());
    if (it == diskDbNames.cend()) {
        return grpc::Status {grpc::NOT_FOUND,
                             "The database could not be found"};
    }

    db::DB* db = db::DB::create();
    db::DBLoader loader {db, FileUtils::Path(_config.getDatabasesPath()) / request->db_name()};
    if (!loader.load()) {
        return grpc::Status {grpc::INTERNAL,
                             "Could not load the database"};
    }

    _databases.emplace(_nextAvailableId, db);
    _dbNameMapping.emplace(request->db_name(), _nextAvailableId);
    reply->mutable_db()->set_id(_nextAvailableId);
    reply->mutable_db()->set_name(request->db_name());

    _nextAvailableId++;

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::UnloadDB(grpc::ServerContext* ctxt,
                                      const UnloadDBRequest* request,
                                      UnloadDBReply* reply) {
    const auto it = _dbNameMapping.find(request->db_name());
    if (it == _dbNameMapping.end()) {
        return grpc::Status {grpc::NOT_FOUND,
                             "The database is not loaded"};
    }
    delete _databases.at(it->second);
    _databases.erase(_databases.find(it->second));
    _dbNameMapping.erase(it);

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::DumpDB(grpc::ServerContext* ctxt,
                                    const DumpDBRequest* request,
                                    DumpDBReply* reply) {

    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const std::string& dbName = std::find_if(_dbNameMapping.cbegin(),
                                             _dbNameMapping.cend(),
                                             [&](const auto& it) {
                                                 return it.second == request->db_id();
                                             })
                                    ->first;

    db::DBDumper dumper {_databases.at(request->db_id()),
                         FileUtils::Path(_config.getDatabasesPath()) / dbName};
    if (!dumper.dump()) {
        return grpc::Status {grpc::INTERNAL,
                             "Could not dump the database"};
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateDB(grpc::ServerContext* ctxt,
                                      const CreateDBRequest* request,
                                      CreateDBReply* reply) {
    std::vector<std::string> diskDbNames;
    listDiskDB(diskDbNames);

    auto it = std::find(diskDbNames.cbegin(), diskDbNames.cend(), request->db_name());
    if (it != diskDbNames.cend()) {
        return grpc::Status {grpc::ALREADY_EXISTS,
                             "The database already exists"};
    }

    if (_dbNameMapping.find(request->db_name()) != _dbNameMapping.end()) {
        return grpc::Status {grpc::ALREADY_EXISTS,
                             "The database is already loaded"};
    }

    db::DB* db = db::DB::create();
    _databases.emplace(_nextAvailableId, db);
    _dbNameMapping.emplace(request->db_name(), _nextAvailableId);

    reply->mutable_db()->set_id(_nextAvailableId);
    reply->mutable_db()->set_name(request->db_name());
    _nextAvailableId++;

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodes(grpc::ServerContext* ctxt,
                                       const ListNodesRequest* request,
                                       ListNodesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    db::DB::NodeRange nodes = db->nodes();
    if (nodes.size() > MAX_ENTITY_COUNT) {
        return grpc::Status {
            grpc::StatusCode::ABORTED,
            "Too many nodes in the database to list all of them ("
                + std::to_string(nodes.size())
                + " in the database, maximum amount is "
                + std::to_string(MAX_ENTITY_COUNT) + ")"};
    }

    reply->mutable_nodes()->Reserve(nodes.size());
    for (const auto& pair : nodes) {
        auto* n = reply->add_nodes();
        n->set_id(pair.first);
        n->set_db_id(request->db_id());
        n->set_net_id(pair.second->getNetwork()->getName().getID());
        n->set_node_type_id(pair.second->getType()->getName().getID());

        n->mutable_out_edge_ids()->Reserve(pair.second->outEdges().size());
        for (const db::Edge* out : pair.second->outEdges()) {
            n->add_out_edge_ids(out->getIndex());
        }

        n->mutable_in_edge_ids()->Reserve(pair.second->inEdges().size());
        for (const db::Edge* in : pair.second->inEdges()) {
            n->add_in_edge_ids(in->getIndex());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodesByID(grpc::ServerContext* ctxt,
                                           const ListNodesByIDRequest* request,
                                           ListNodesByIDReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    reply->mutable_nodes()->Reserve(request->node_ids_size());

    for (const auto& id : request->node_ids()) {
        db::Node* n = db->getNode((db::DBIndex)id);
        if (!n) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "Node '" + std::to_string(id) + "' was not found"};
        }

        auto rn = reply->add_nodes();
        rn->set_id(id);
        rn->set_db_id(request->db_id());
        rn->set_net_id(n->getNetwork()->getName().getID());
        rn->set_node_type_id(n->getType()->getName().getID());

        rn->mutable_out_edge_ids()->Reserve(n->outEdges().size());
        for (const db::Edge* out : n->outEdges()) {
            rn->add_out_edge_ids(out->getIndex());
        }

        rn->mutable_in_edge_ids()->Reserve(n->inEdges().size());
        for (const db::Edge* in : n->inEdges()) {
            rn->add_in_edge_ids(in->getIndex());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdges(grpc::ServerContext* ctxt,
                                       const ListEdgesRequest* request,
                                       ListEdgesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    db::DB::EdgeRange edges = db->edges();

    if (edges.size() > MAX_ENTITY_COUNT) {
        return grpc::Status {
            grpc::StatusCode::ABORTED,
            "Too many edges in the database to list all of them ("
                + std::to_string(edges.size())
                + " in the database, maximum amount is "
                + std::to_string(MAX_ENTITY_COUNT) + ")"};
    }

    reply->mutable_edges()->Reserve(edges.size());
    for (const auto& pair : edges) {
        auto* e = reply->add_edges();
        e->set_id(pair.first);
        e->set_db_id(request->db_id());
        e->set_source_id(pair.second->getSource()->getIndex());
        e->set_target_id(pair.second->getTarget()->getIndex());
        e->set_edge_type_id(pair.second->getType()->getName().getID());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdgesByID(grpc::ServerContext* ctxt,
                                           const ListEdgesByIDRequest* request,
                                           ListEdgesByIDReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    reply->mutable_edges()->Reserve(request->edge_ids_size());

    for (const auto& id : request->edge_ids()) {
        db::Edge* e = db->getEdge((db::DBIndex)id);
        if (!e) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "Edge '" + std::to_string(id) + "' was not found"};
        }

        auto re = reply->add_edges();
        re->set_id(id);
        re->set_db_id(request->db_id());
        re->set_source_id(e->getSource()->getIndex());
        re->set_target_id(e->getTarget()->getIndex());
        re->set_edge_type_id(e->getType()->getName().getID());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodeTypes(grpc::ServerContext* ctxt,
                                           const ListNodeTypesRequest* request,
                                           ListNodeTypesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());

    for (const db::NodeType* nt : db->nodeTypes()) {
        auto* rpcNodeType = reply->add_node_types();
        rpcNodeType->set_db_id(request->db_id());
        rpcNodeType->set_id(nt->getName().getID());
        rpcNodeType->set_name(nt->getName().getSharedString()->getString());

        for (const db::EdgeType* et : nt->inEdgeTypes()) {
            rpcNodeType->add_in_edge_ids(et->getName().getID());
        }

        for (const db::EdgeType* et : nt->outEdgeTypes()) {
            rpcNodeType->add_out_edge_ids(et->getName().getID());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodeTypesByID(grpc::ServerContext* ctxt,
                                               const ListNodeTypesByIDRequest* request,
                                               ListNodeTypesByIDReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    reply->mutable_node_types()->Reserve(request->node_type_ids_size());

    for (const auto& id : request->node_type_ids()) {
        db::NodeType* nt = db->getNodeType((db::DBIndex)id);
        if (!nt) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "NodeType '" + std::to_string(id) + "' was not found"};
        }
        auto* rpcNodeType = reply->add_node_types();
        rpcNodeType->set_db_id(request->db_id());
        rpcNodeType->set_id(nt->getName().getID());
        rpcNodeType->set_name(nt->getName().getSharedString()->getString());

        for (const db::EdgeType* et : nt->inEdgeTypes()) {
            rpcNodeType->add_in_edge_ids(et->getName().getID());
        }

        for (const db::EdgeType* et : nt->outEdgeTypes()) {
            rpcNodeType->add_out_edge_ids(et->getName().getID());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdgeTypes(grpc::ServerContext* ctxt,
                                           const ListEdgeTypesRequest* request,
                                           ListEdgeTypesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());

    for (const db::EdgeType* et : db->edgeTypes()) {
        auto* rpcEdgeType = reply->add_edge_types();
        rpcEdgeType->set_db_id(request->db_id());
        rpcEdgeType->set_id(et->getName().getID());
        rpcEdgeType->set_name(et->getName().getSharedString()->getString());

        for (const db::NodeType* nt : et->sourceTypes()) {
            rpcEdgeType->add_source_ids(nt->getName().getID());
        }

        for (const db::NodeType* nt : et->targetTypes()) {
            rpcEdgeType->add_target_ids(nt->getName().getID());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdgeTypesByID(grpc::ServerContext* ctxt,
                                           const ListEdgeTypesByIDRequest* request,
                                           ListEdgeTypesByIDReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    reply->mutable_edge_types()->Reserve(request->edge_type_ids_size());

    for (const auto& id : request->edge_type_ids()) {
        db::EdgeType* et = db->getEdgeType((db::DBIndex)id);
        if (!et) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "EdgeType '" + std::to_string(id) + "' was not found"};
        }
        auto* rpcEdgeType = reply->add_edge_types();
        rpcEdgeType->set_db_id(request->db_id());
        rpcEdgeType->set_id(et->getName().getID());
        rpcEdgeType->set_name(et->getName().getSharedString()->getString());

        for (const db::NodeType* nt : et->sourceTypes()) {
            rpcEdgeType->add_source_ids(nt->getName().getID());
        }

        for (const db::NodeType* nt : et->targetTypes()) {
            rpcEdgeType->add_target_ids(nt->getName().getID());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodesFromNetwork(grpc::ServerContext* ctxt,
                                                  const ListNodesFromNetworkRequest* request,
                                                  ListNodesFromNetworkReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    const db::DBIndex net_id {request->net_id()};
    const db::Network* net = db->getNetwork(net_id);
    db::Network::NodeRange nodes = net->nodes();
    if (nodes.size() > MAX_ENTITY_COUNT) {
        return grpc::Status {
            grpc::StatusCode::ABORTED,
            "Too many nodes in the database to list all of them ("
                + std::to_string(nodes.size())
                + " in the database, maximum amount is "
                + std::to_string(MAX_ENTITY_COUNT) + ")"};
    }

    reply->mutable_nodes()->Reserve(nodes.size());
    for (const auto& [id, node] : nodes) {
        auto* n = reply->add_nodes();
        n->set_id(node->getIndex());
        n->set_db_id(request->db_id());
        n->set_net_id(net_id);
        n->set_node_type_id(node->getType()->getName().getID());

        n->mutable_out_edge_ids()->Reserve(node->outEdges().size());
        for (const db::Edge* out : node->outEdges()) {
            n->add_out_edge_ids(out->getIndex());
        }

        n->mutable_in_edge_ids()->Reserve(node->inEdges().size());
        for (const db::Edge* in : node->inEdges()) {
            n->add_in_edge_ids(in->getIndex());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNodesByIDFromNetwork(grpc::ServerContext* ctxt,
                                                      const ListNodesByIDFromNetworkRequest* request,
                                                      ListNodesByIDFromNetworkReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    const db::DBIndex net_id {request->net_id()};
    const db::Network* net = db->getNetwork(net_id);

    reply->mutable_nodes()->Reserve(request->node_ids_size());

    for (const auto& id : request->node_ids()) {
        db::Node* node = net->getNode((db::DBIndex)id);
        if (!node) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "Node '" + std::to_string(id) + "' was not found"};
        }
        auto* n = reply->add_nodes();
        n->set_id(node->getIndex());
        n->set_db_id(request->db_id());
        n->set_net_id(net_id);
        n->set_node_type_id(node->getType()->getName().getID());

        n->mutable_out_edge_ids()->Reserve(node->outEdges().size());
        for (const db::Edge* out : node->outEdges()) {
            n->add_out_edge_ids(out->getIndex());
        }

        n->mutable_in_edge_ids()->Reserve(node->inEdges().size());
        for (const db::Edge* in : node->inEdges()) {
            n->add_in_edge_ids(in->getIndex());
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdgesFromNetwork(grpc::ServerContext* ctxt,
                                                  const ListEdgesFromNetworkRequest* request,
                                                  ListEdgesFromNetworkReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    const db::DBIndex net_id {request->net_id()};
    const db::Network* net = db->getNetwork(net_id);
    db::Network::EdgeRange edges = net->edges();

    if (edges.size() > MAX_ENTITY_COUNT) {
        return grpc::Status {
            grpc::StatusCode::ABORTED,
            "Too many edges in the database to list all of them ("
                + std::to_string(edges.size())
                + " in the database, maximum amount is "
                + std::to_string(MAX_ENTITY_COUNT) + ")"};
    }

    reply->mutable_edges()->Reserve(edges.size());
    for (const auto& [id, edge] : edges) {
        auto* e = reply->add_edges();
        e->set_id(edge->getIndex());
        e->set_db_id(request->db_id());
        e->set_source_id(edge->getSource()->getIndex());
        e->set_target_id(edge->getTarget()->getIndex());
        e->set_edge_type_id(edge->getType()->getName().getID());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEdgesByIDFromNetwork(grpc::ServerContext* ctxt,
                                                      const ListEdgesByIDFromNetworkRequest* request,
                                                      ListEdgesByIDFromNetworkReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());
    const db::DBIndex net_id {request->net_id()};
    const db::Network* net = db->getNetwork(net_id);

    reply->mutable_edges()->Reserve(request->edge_ids_size());

    for (const auto& id : request->edge_ids()) {
        db::Edge* edge = net->getEdge((db::DBIndex)id);
        if (!edge) {
            return grpc::Status {grpc::StatusCode::NOT_FOUND,
                                 "Edge '" + std::to_string(id) + "' was not found"};
        }
        auto* e = reply->add_edges();
        e->set_id(edge->getIndex());
        e->set_db_id(request->db_id());
        e->set_source_id(edge->getSource()->getIndex());
        e->set_target_id(edge->getTarget()->getIndex());
        e->set_edge_type_id(edge->getType()->getName().getID());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListNetworks(grpc::ServerContext* ctxt,
                                          const ListNetworksRequest* request,
                                          ListNetworksReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    const db::DB* db = _databases.at(request->db_id());

    for (const db::Network* net : db->networks()) {
        auto* rpcNet = reply->add_networks();
        rpcNet->set_db_id(request->db_id());
        rpcNet->set_id(net->getName().getID());
        rpcNet->set_name(net->getName().getSharedString()->getString());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListEntityProperties(grpc::ServerContext* ctxt,
                                                  const ListEntityPropertiesRequest* request,
                                                  ListEntityPropertiesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::DBEntity* e {nullptr};

    switch (request->entity_type()) {
        case EntityType::NODE: {
            e = db->getNode((db::DBIndex)request->entity_id());
            break;
        }
        case EntityType::EDGE: {
            e = db->getEdge((db::DBIndex)request->entity_id());
            break;
        }
        default:
            return grpc::Status {grpc::INTERNAL, "EntityType is invalid"};
    }

    for (const auto& pair : e->properties()) {
        auto* p = reply->add_properties();
        p->set_property_type_name(pair.first->getName().getSharedString()->getString());
        switch (pair.second.getType().getKind()) {
            case db::ValueType::VK_INT: {
                p->set_value_type(ValueType::INT);
                p->set_int64(pair.second.getInt());
                break;
            }
            case db::ValueType::VK_UNSIGNED: {
                p->set_value_type(ValueType::UNSIGNED);
                p->set_uint64(pair.second.getUint());
                break;
            }
            case db::ValueType::VK_STRING: {
                p->set_value_type(ValueType::STRING);
                p->set_string(pair.second.getString());
                break;
            }
            case db::ValueType::VK_BOOL: {
                p->set_value_type(ValueType::BOOL);
                p->set_boolean(pair.second.getBool());
                break;
            }
            case db::ValueType::VK_DECIMAL: {
                p->set_value_type(ValueType::DECIMAL);
                p->set_decimal(pair.second.getDouble());
                break;
            }
            default: {
                return grpc::Status {grpc::StatusCode::INTERNAL, "Invalid property"};
            }
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateNodeType(grpc::ServerContext* ctxt,
                                            const CreateNodeTypeRequest* request,
                                            CreateNodeTypeReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::NodeType* nt = wb.createNodeType(db->getString(request->name()));

    if (!nt) {
        return grpc::Status {
            grpc::StatusCode::INTERNAL,
            "Could not create the NodeType " + request->name()
                + ". Does it already exist ?"};
    }

    reply->mutable_node_type()->set_db_id(request->db_id());
    reply->mutable_node_type()->set_id(nt->getIndex());
    reply->mutable_node_type()->set_name(request->name());

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateEdgeType(grpc::ServerContext* ctxt,
                                            const CreateEdgeTypeRequest* request,
                                            CreateEdgeTypeReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    std::vector<db::NodeType*> sources(request->source_ids().size());
    std::vector<db::NodeType*> targets(request->target_ids().size());
    size_t i = 0;
    for (size_t id : request->source_ids()) {
        sources[i] = db->getNodeType((db::DBIndex)id);
        i++;
    }
    i = 0;
    for (size_t id : request->target_ids()) {
        targets[i] = db->getNodeType((db::DBIndex)id);
        i++;
    }
    db::EdgeType* et = wb.createEdgeType(db->getString(request->name()),
                                         sources, targets);
    if (!et) {
        return grpc::Status {
            grpc::StatusCode::INTERNAL,
            "Could not create the EdgeType " + request->name()
                + ". Does it already exist ?"};
    }

    reply->mutable_edge_type()->set_db_id(request->db_id());
    reply->mutable_edge_type()->set_id(et->getName().getID());
    reply->mutable_edge_type()->set_name(request->name());

    for (const db::NodeType* nt : sources) {
        reply->mutable_edge_type()->add_source_ids(nt->getName().getID());
    }

    for (const db::NodeType* nt : targets) {
        reply->mutable_edge_type()->add_target_ids(nt->getName().getID());
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateNetwork(grpc::ServerContext* ctxt,
                                           const CreateNetworkRequest* request,
                                           CreateNetworkReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::Network* net = wb.createNetwork(db->getString(request->name()));
    if (!net) {
        return grpc::Status {grpc::StatusCode::INTERNAL,
                             "Could not create network. Does it already exist ?"};
    }
    reply->mutable_network()->set_db_id(request->db_id());
    reply->mutable_network()->set_id(net->getName().getID());
    reply->mutable_network()->set_name(net->getName().getSharedString()->getString());

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateNode(grpc::ServerContext* ctxt,
                                        const CreateNodeRequest* request,
                                        CreateNodeReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::NodeType* nt = db->getNodeType((db::DBIndex)request->node_type_id());
    if (!nt) {
        return grpc::Status {grpc::StatusCode::NOT_FOUND, "NodeType is invalid"};
    }
    db::Network* net = db->getNetwork((db::DBIndex)request->net_id());
    if (!net) {
        return grpc::Status {grpc::StatusCode::NOT_FOUND, "Network is invalid"};
    }
    const db::StringRef nodeName = db->getString(request->name());
    db::Node* node = wb.createNode(net, nt, nodeName);
    if (!node) {
        return grpc::Status {grpc::StatusCode::INTERNAL, "Could not create node"};
    }

    auto n = new ::Node;
    n->set_db_id(request->db_id());
    n->set_net_id(request->net_id());
    n->set_id(node->getIndex());
    n->set_name(request->name());
    n->set_node_type_id(request->node_type_id());
    reply->set_allocated_node(n);

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::CreateEdge(grpc::ServerContext* ctxt,
                                        const CreateEdgeRequest* request,
                                        CreateEdgeReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::EdgeType* et = db->getEdgeType((db::DBIndex)request->edge_type_id());
    if (!et) {
        return grpc::Status {grpc::NOT_FOUND, "Edge type does not exist"};
    }
    db::Node* source = db->getNode((db::DBIndex)request->source_id());
    if (!source) {
        return grpc::Status {grpc::NOT_FOUND, "Source node does not exist"};
    }
    db::Node* target = db->getNode((db::DBIndex)request->target_id());
    if (!target) {
        return grpc::Status {grpc::NOT_FOUND, "Target node does not exist"};
    }
    db::Edge* edge = wb.createEdge(et, source, target);
    if (!edge) {
        return grpc::Status {grpc::INVALID_ARGUMENT, "Could not create the edge"};
    }

    auto e = new ::Edge;
    e->set_db_id(request->db_id());
    e->set_id(edge->getIndex());
    e->set_edge_type_id(request->edge_type_id());
    e->set_source_id(source->getIndex());
    e->set_target_id(target->getIndex());
    reply->set_allocated_edge(e);

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::AddEntityProperty(grpc::ServerContext* ctxt,
                                               const AddEntityPropertyRequest* request,
                                               AddEntityPropertyReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::DBEntity* e {nullptr};
    switch (request->entity_type()) {
        case EntityType::NODE: {
            e = db->getNode((db::DBIndex)request->entity_id());
            break;
        }
        case EntityType::EDGE: {
            e = db->getEdge((db::DBIndex)request->entity_id());
            break;
        }
        default:
            return grpc::Status {grpc::INTERNAL, "EntityType is invalid"};
    }

    auto& p = request->property();
    const db::PropertyType* pt = e->getType()->getPropertyType(
        db->getString(p.property_type_name()));

    auto* rpcProp = reply->mutable_property();
    rpcProp->set_property_type_name(p.property_type_name());
    rpcProp->set_value_type(p.value_type());

    switch (p.value_type()) {
        case ValueType::INT: {
            wb.setProperty(e, {pt, db::Value::createInt(p.int64())});
            rpcProp->set_int64(p.int64());
            break;
        }
        case ValueType::UNSIGNED: {
            wb.setProperty(e, {pt, db::Value::createUnsigned(p.uint64())});
            rpcProp->set_uint64(p.uint64());
            break;
        }
        case ValueType::STRING: {
            std::string s = p.string();
            wb.setProperty(e, {pt, db::Value::createString(std::move(s))});
            rpcProp->set_string(p.string());
            break;
        }
        case ValueType::BOOL: {
            wb.setProperty(e, {pt, db::Value::createBool(p.boolean())});
            rpcProp->set_boolean(p.boolean());
            break;
        }
        case ValueType::DECIMAL: {
            wb.setProperty(e, {pt, db::Value::createDouble(p.decimal())});
            rpcProp->set_decimal(p.decimal());
            break;
        }
        default: {
            return grpc::Status {grpc::INTERNAL, "The value type is invalid"};
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::AddPropertyType(grpc::ServerContext* ctxt,
                                             const AddPropertyTypeRequest* request,
                                             AddPropertyTypeReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::Writeback wb {db};
    db::DBEntityType* et {nullptr};

    auto* rpcPt = reply->mutable_property_type();
    rpcPt->set_name(request->property_type().name());
    rpcPt->set_value_type(request->property_type().value_type());
    switch (request->entity_type()) {
        case EntityType::NODE: {
            et = db->getNodeType((db::DBIndex)request->entity_type_id());
            break;
        }
        case EntityType::EDGE: {
            et = db->getEdgeType((db::DBIndex)request->entity_type_id());
            break;
        }
        default:
            return grpc::Status {grpc::INTERNAL, "EntityType is invalid"};
    }

    const PropertyType& pt = request->property_type();
    const db::StringRef ptName = db->getString(pt.name());

    switch (pt.value_type()) {
        case ValueType::INT: {
            wb.addPropertyType(et, ptName, db::ValueType::intType());
            break;
        }
        case ValueType::UNSIGNED: {
            wb.addPropertyType(et, ptName, db::ValueType::unsignedType());
            break;
        }
        case ValueType::STRING: {
            wb.addPropertyType(et, ptName, db::ValueType::stringType());
            break;
        }
        case ValueType::BOOL: {
            wb.addPropertyType(et, ptName, db::ValueType::boolType());
            break;
        }
        case ValueType::DECIMAL: {
            wb.addPropertyType(et, ptName, db::ValueType::decimalType());
            break;
        }
        default: {
            return grpc::Status {grpc::INTERNAL, "The value type is invalid"};
        }
    }

    return grpc::Status::OK;
}

grpc::Status APIServiceImpl::ListPropertyTypes(grpc::ServerContext* ctxt,
                                               const ListPropertyTypesRequest* request,
                                               ListPropertyTypesReply* reply) {
    if (!isDBValid(request->db_id())) {
        return invalidDBStatus;
    }

    db::DB* db = _databases.at(request->db_id());
    db::DBEntityType* et {nullptr};

    switch (request->entity_type()) {
        case EntityType::NODE: {
            et = db->getNodeType((db::DBIndex)request->entity_type_id());
            break;
        }
        case EntityType::EDGE: {
            et = db->getEdgeType((db::DBIndex)request->entity_type_id());
            break;
        }
        default:
            return grpc::Status {grpc::INTERNAL, "EntityType is invalid"};
    }

    for (const db::PropertyType* pt : et->propertyTypes()) {
        auto* rpcPt = reply->add_property_types();
        rpcPt->set_name(pt->getName().getSharedString()->getString());

        switch (pt->getValueType().getKind()) {
            case db::ValueType::VK_INT: {
                rpcPt->set_value_type(::ValueType::INT);
                break;
            }
            case db::ValueType::VK_UNSIGNED: {
                rpcPt->set_value_type(::ValueType::UNSIGNED);
                break;
            }
            case db::ValueType::VK_STRING: {
                rpcPt->set_value_type(::ValueType::STRING);
                break;
            }
            case db::ValueType::VK_BOOL: {
                rpcPt->set_value_type(::ValueType::BOOL);
                break;
            }
            case db::ValueType::VK_DECIMAL: {
                rpcPt->set_value_type(::ValueType::DECIMAL);
                break;
            }
            default: {
                return grpc::Status {grpc::INTERNAL, "The value type is invalid"};
            }
        }
    }

    return grpc::Status::OK;
}

void APIServiceImpl::listDiskDB(std::vector<std::string>& databaseNames) {
    std::vector<FileUtils::Path> paths;
    FileUtils::Path databasesPath {_config.getDatabasesPath()};
    FileUtils::listFiles(databasesPath, paths);

    databaseNames.reserve(paths.size());

    for (const FileUtils::Path& dbPath : paths) {
        databaseNames.push_back(dbPath.filename().string());
    }
}

bool APIServiceImpl::isDBValid(size_t id) const {
    return _databases.find(id) != _databases.end();
}
