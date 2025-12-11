#include "DBServerProcessor.h"

#include <nlohmann/json.hpp>

#include "TuringDB.h"
#include "Graph.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/EdgeView.h"
#include "QueryInterpreter.h"
#include "SystemManager.h"
#include "Path.h"

#include "ListNodesExecutor.h"
#include "ExploreNodeEdgesExecutor.h"

#include "DBThreadContext.h"
#include "HTTPParser.h"
#include "DBURIParser.h"
#include "Endpoints.h"
#include "HTTP.h"
#include "HTTPResponseWriter.h"
#include "TCPConnection.h"
#include "JsonEncoder.h"

using namespace db;

DBServerProcessor::DBServerProcessor(TuringDB& db,
                                     net::TCPConnection& connection)
    : _writer(&connection.getWriter()),
    _db(db),
    _connection(connection)
{
}

DBServerProcessor::~DBServerProcessor() {
}

void DBServerProcessor::process(net::AbstractThreadContext* abstractContext) {
    _threadContext = static_cast<DBThreadContext*>(abstractContext);
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();

    const auto& httpInfo = parser.getHttpInfo();
    if (httpInfo._method != net::HTTP::Method::POST) {
        _writer.writeHttpError(net::HTTP::Status::METHOD_NOT_ALLOWED);
        return;
    }

    switch ((Endpoint)httpInfo._endpoint) {
        case Endpoint::QUERY: {
             query();
        }
        break;
        case Endpoint::LOAD_GRAPH: {
             load_graph();
        }
        break;
        case Endpoint::GET_GRAPH_STATUS: {
             get_graph_status();
        }
        break;
        case Endpoint::IS_GRAPH_LOADED: {
             is_graph_loaded();
        }
        break;
        case Endpoint::IS_GRAPH_LOADING: {
             is_graph_loading();
        }
        break;
        case Endpoint::LIST_LOADED_GRAPHS: {
             list_loaded_graphs();
        }
        break;
        case Endpoint::LIST_AVAIL_GRAPHS: {
             list_avail_graphs();
        }
        break;
        case Endpoint::LIST_LABELS: {
             list_labels();
        }
        break;
        case Endpoint::LIST_PROPERTY_TYPES: {
             list_property_types();
        }
        break;
        case Endpoint::LIST_EDGE_TYPES: {
             list_edge_types();
        }
        break;
        case Endpoint::LIST_NODES: {
             list_nodes();
        }
        break;
        case Endpoint::GET_NODE_PROPERTIES: {
             get_node_properties();
        }
        break;
        case Endpoint::GET_NEIGHBORS: {
             get_neighbors();
        }
        break;
        case Endpoint::GET_NODES: {
             get_nodes();
        }
        break;
        case Endpoint::GET_NODE_EDGES: {
             get_node_edges();
        }
        break;
        case Endpoint::GET_EDGES: {
             get_edges();
        }
        break;
        case Endpoint::EXPLORE_NODE_EDGES: {
             explore_node_edges();
        }
        break;
        case Endpoint::HISTORY: {
             history();
        }
        break;
        default: {
            _writer.writeHttpError(net::HTTP::Status::NOT_FOUND);
        }
        break;
    }

    _threadContext->getLocalMemory().clear();
}

const Graph* DBServerProcessor::getRequestedGraph() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    const auto& sysMan = _db.getSystemManager();
    const auto& httpInfo = parser.getHttpInfo();
    const std::string_view graphNameView = httpInfo._params[(size_t)DBHTTPParams::graph];
    return graphNameView.empty()
             ? sysMan.getDefaultGraph()
             : sysMan.getGraph(std::string(graphNameView));
}

const net::HTTP::Info& DBServerProcessor::getHttpInfo() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    return parser.getHttpInfo();
}

void DBServerProcessor::query() {
    LocalMemory& mem = _threadContext->getLocalMemory();
    const auto& httpInfo = getHttpInfo();

    const auto transactionInfo = getTransactionInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto res = _db.query(
        httpInfo._payload,
        transactionInfo.graphName,
        &mem,
        [&](const Block& block) { JsonEncoder::writeBlock(payload, block); },
        [&](const QueryCommand* cmd) { JsonEncoder::writeHeader(payload, cmd); },
        transactionInfo.commit,
        transactionInfo.change);

    if (!res.isOk()) {
        if (res.getStatus() == QueryStatus::Status::EXEC_ERROR) {
            payload.end();
        }
        payload.key("error");
        const std::string errorType = std::string(QueryStatusDescription::value(res.getStatus()));
        payload.value(errorType);

        const std::string& errorMsg =
            res.getError().empty() ? "No error message available." : res.getError();

        payload.key("error_details");
        payload.value(errorMsg);

        return;
    }

    payload.end();

    payload.key("time");
    payload.value(res.getTotalTime().count());
}

void DBServerProcessor::load_graph() {
    auto& sys = _db.getSystemManager();

    const auto transactionInfo = getTransactionInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    if (!sys.loadGraph(transactionInfo.graphName)) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::GRAPH_LOAD_ERROR));
        return;
    }
}

void DBServerProcessor::get_graph_status() {
    const auto& sysMan = _db.getSystemManager();

    const auto transactionInfo = getTransactionInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const Graph* graph = getRequestedGraph();

    payload.key("data");
    payload.obj();

    payload.key("isLoaded");
    payload.value(graph != nullptr);

    payload.key("isLoading");
    payload.value(sysMan.isGraphLoading(transactionInfo.graphName));

    if (graph != nullptr) {
        const auto transaction = graph->openTransaction();
        const auto reader = transaction.readGraph();
        payload.key("nodeCount");
        payload.value(reader.getNodeCount());
        payload.key("edgeCount");
        payload.value(reader.getEdgeCount());
    }
}

void DBServerProcessor::history() {
    const auto& httpInfo = getHttpInfo();

    const auto graphNameView = httpInfo._params[(size_t)DBHTTPParams::graph];
    if (graphNameView.empty()) {
        _writer.writeHttpError(net::HTTP::Status::BAD_REQUEST);
        return;
    }

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);
    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    payload.key("data");
    payload.obj();

    static constexpr auto formatCommitLog = [](PayloadWriter& payload, const CommitView& commit) {
        std::string str = fmt::format("Commit: {:x}", commit.hash().get());
        if (commit.isHead()) {
            str += " (HEAD)";
        }
        str += "\\n";

        size_t i = 0;
        for (const auto& part : commit.dataparts()) {
            str += fmt::format(" - Part {}: {} nodes, {} edges\\n",
                               i + 1, part->getNodeContainerSize(), part->getEdgeContainerSize());
            i++;
        }
        payload.value(str);
    };

    payload.key("commits");
    payload.arr();

    for (const auto& commit : transaction.value().viewGraph().commits()) {
        formatCommitLog(payload, commit);
    }
}

void DBServerProcessor::is_graph_loaded() {
    const auto& httpInfo = getHttpInfo();

    const auto graphNameView = httpInfo._params[(size_t)DBHTTPParams::graph];
    if (graphNameView.empty()) {
        _writer.writeHttpError(net::HTTP::Status::BAD_REQUEST);
        return;
    }

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const Graph* graph = _db.getSystemManager().getGraph(std::string(graphNameView));

    payload.key("data");
    payload.value(graph != nullptr);
}

void DBServerProcessor::is_graph_loading() {
    const auto& httpInfo = getHttpInfo();

    const auto graphNameView = httpInfo._params[(size_t)DBHTTPParams::graph];
    if (graphNameView.empty()) {
        _writer.writeHttpError(net::HTTP::Status::BAD_REQUEST);
        return;
    }

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const bool isLoading = _db.getSystemManager().isGraphLoading(std::string(graphNameView));
    payload.key("data");
    payload.value(isLoading);
}

void DBServerProcessor::list_loaded_graphs() {
    LocalMemory& mem = _threadContext->getLocalMemory();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());
    PayloadWriter payload(_writer.getWriter());

    payload.obj();
    payload.key("data");
    payload.arr();

    const auto res = _db.query("LIST GRAPH",
                               "",
                               &mem,
                               [&](const Block& block) { JsonEncoder::writeBlock(payload, block); });

    if (!res.isOk()) {
        payload.end();
        payload.key("error");
        payload.value(QueryStatusDescription::value(res.getStatus()));
    }
}

void DBServerProcessor::list_avail_graphs() {
    std::vector<fs::Path> list;
    _db.getSystemManager().listAvailableGraphs(list);

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());

    payload.obj();
    payload.key("data");
    payload.arr();

    for (const auto& path : list) {
        payload.value(path.filename());
    }
}

void DBServerProcessor::list_labels() {
    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    const auto& reqBody = getHttpInfo()._payload;
    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();
    const auto& labels = reader.getMetadata().labels();
    LabelSet labelset;

    if (!reqBody.empty()) {
        try {
            const auto json = nlohmann::json::parse(reqBody);

            // Labels
            const auto labelsIt = json.find("currentLabels");
            if (labelsIt != json.end()) {
                for (const auto& label : labelsIt.value()) {
                    const auto& labelName = label.get<std::string>();
                    const auto labelID = labels.get(labelName);
                    if (!labelID) {
                        continue;
                    }

                    labelset.set(labelID.value());
                }
            }
        } catch (const std::exception& e) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
            return;
        }
    }

    payload.key("data");
    payload.obj();

    const size_t labelCount = labels.getCount();
    std::vector<LabelID> remainingLabels;

    payload.key("labels");
    payload.arr();

    for (size_t i = 0; i < labelCount; i++) {
        if (labelset.hasLabel(i)) {
            continue;
        }
        remainingLabels.push_back(i);

        payload.value(labels.getName((LabelID)i));
    }

    payload.end();

    payload.key("nodeCounts");
    payload.arr();

    LabelSet curLabelset = labelset;
    for (size_t i = 0; i < remainingLabels.size(); i++) {
        curLabelset = labelset;
        curLabelset.set(remainingLabels[i]);

        payload.value(reader.getNodeCountMatchingLabelset(LabelSetHandle {curLabelset}));
    }
}

void DBServerProcessor::list_property_types() {
    const auto& httpInfo = getHttpInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());
    std::vector<NodeID::Type> nodeIDs;

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();

    if (!httpInfo._payload.empty()) {
        try {
            const auto json = nlohmann::json::parse(httpInfo._payload);

            // NodeIDs
            const auto nodeIDsIt = json.find("nodeIDs");
            if (nodeIDsIt != json.end()) {
                nodeIDs = nodeIDsIt->get<std::vector<NodeID::Type>>();
            }
        } catch (const std::exception& e) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        }
    }

    payload.key("data");
    payload.arr();

    const auto& propTypes = reader.getMetadata().propTypes();

    // If no node IDs provided, list all property types
    if (nodeIDs.empty()) {
        const size_t propTypeCount = propTypes.getCount();
        for (size_t i = 0; i < propTypeCount; i++) {
            payload.value(propTypes.getName((PropertyTypeID)i));
        }
        return;
    }

    // Else only show the node's property types
    const size_t propTypeCount = propTypes.getCount();
    for (PropertyTypeID ptID = 0; ptID < propTypeCount; ptID++) {
        for (const auto nodeID : nodeIDs) {
            if (reader.nodeHasProperty(ptID, nodeID)) {
                payload.value(propTypes.getName((PropertyTypeID)ptID));
                break;
            }
        }
    }
}

void DBServerProcessor::list_edge_types() {
    const auto& httpInfo = getHttpInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());
    std::vector<EdgeID::Type> edgeIDs;

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();

    if (!httpInfo._payload.empty()) {
        try {
            const auto json = nlohmann::json::parse(httpInfo._payload);

            // EdgeIDs
            const auto edgeIDsIt = json.find("edgeIDs");
            if (edgeIDsIt != json.end()) {
                edgeIDs = edgeIDsIt->get<std::vector<EdgeID::Type>>();
            }
        } catch (const std::exception& e) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        }
    }

    payload.key("data");
    payload.arr();

    const auto& edgeTypes = reader.getMetadata().edgeTypes();

    // If no edge IDs provided, list all edge types
    if (edgeIDs.empty()) {
        const size_t edgeTypeCount = edgeTypes.getCount();
        for (size_t i = 0; i < edgeTypeCount; i++) {
            payload.value(edgeTypes.getName((EdgeTypeID)i));
        }
        return;
    }

    // Else only show the edges' property types
    const size_t edgeTypeCount = edgeTypes.getCount();
    for (EdgeTypeID etID = 0; etID < edgeTypeCount; etID++) {
        for (const auto edgeID : edgeIDs) {
            auto currentTypeID = reader.getEdgeTypeID(edgeID);
            if (etID == currentTypeID) {
                payload.value(edgeTypes.getName((EdgeTypeID)etID));
                break;
            }
        }
    }
}

void DBServerProcessor::list_nodes() {
    const auto& httpInfo = getHttpInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();

    const GraphMetadata& metadata = reader.getMetadata();
    const LabelMap& labels = metadata.labels();
    const PropertyTypeMap& propTypes = metadata.propTypes();

    payload.setMetadata(&metadata);

    ListNodesExecutor executor(reader, payload);

    try {
        const auto json = nlohmann::json::parse(httpInfo._payload);

        // Labels
        const auto labelsIt = json.find("labels");
        if (labelsIt != json.end()) {
            for (const auto& label : labelsIt.value()) {
                const auto& labelName = label.get<std::string>();
                const auto labelID = labels.get(labelName);
                if (!labelID) {
                    continue;
                }

                executor.addLabel(labelID.value());
            }
        }

        // Skip
        const auto skipIt = json.find("skip");
        if (skipIt != json.end()) {
            executor.setSkip(skipIt.value());
        }

        // Limit
        const auto limitIt = json.find("limit");
        if (limitIt != json.end()) {
            executor.setLimit(limitIt.value());
        }

        // Properties
        const auto propertiesIt = json.find("properties");
        if (propertiesIt != json.end()) {
            for (const auto& [type, expr] : propertiesIt->items()) {
                const auto pType = propTypes.get(type);
                if (!pType) {
                    continue;
                }

                if (pType.value()._valueType != ValueType::String) {
                    continue;
                }

                auto str = expr.get<std::string>();
                std::transform(str.begin(), str.end(), str.begin(), [](char c) { return std::tolower(c); });
                executor.addPropertyFilter(pType.value()._id, std::move(str));
            }
        }

    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    payload.key("data");
    payload.obj();

    executor.run();

    payload.end();
    payload.key("nodeCount");
    payload.value(executor.nodeCount());
    payload.key("reachedEnd");
    payload.value(executor.reachedEnd());
}

void DBServerProcessor::get_node_properties() {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    LocalMemory& mem = _threadContext->getLocalMemory();
    const auto& httpInfo = parser.getHttpInfo();
    const std::string_view reqBody = httpInfo._payload;

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();

    ColumnVector<NodeID>* nodeIDs = mem.alloc<ColumnVector<NodeID>>();
    std::vector<std::string> properties;
    const auto& propTypes = reader.getMetadata().propTypes();

    try {
        const auto json = nlohmann::json::parse(reqBody);

        // NodeIDs
        const auto nodeIDsIt = json.find("nodeIDs");
        if (nodeIDsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        const auto& nodeIDsVec = nodeIDsIt->get<std::vector<NodeID::Type>>();
        nodeIDs->reserve(nodeIDsVec.size());
        for (const NodeID::Type nodeID : nodeIDsVec) {
            nodeIDs->push_back(nodeID);
        }

        // Property types
        const auto propsIt = json.find("properties");
        if (propsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        properties = propsIt->get<std::vector<std::string>>();
        if (properties.empty()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }
    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    payload.key("data");
    payload.obj();

    for (const auto& propName : properties) {
        const auto ptype = propTypes.get(propName);
        if (!ptype) {
            continue;
        }

        payload.key(propName);
        payload.obj();

        const auto treat = [&]<SupportedType TypeT> {
            const auto range = reader.getNodeProperties<TypeT>(ptype.value()._id, nodeIDs);
            for (auto it = range.begin(); it.isValid(); it.next()) {
                payload.key(it.getCurrentEntityID());
                payload.value(it.get());
            }
        };

        switch (ptype.value()._valueType) {
            case db::ValueType::UInt64: {
                treat.template operator()<types::UInt64>();
                break;
            }
            case db::ValueType::Int64: {
                treat.template operator()<types::Int64>();
                break;
            }
            case db::ValueType::Double: {
                treat.template operator()<types::Double>();
                break;
            }
            case db::ValueType::Bool: {
                treat.template operator()<types::Bool>();
                break;
            }
            case db::ValueType::String: {
                treat.template operator()<types::String>();
                break;
            }
            case db::ValueType::Invalid:
            case db::ValueType::_SIZE: {
                _writer.writeHttpError(net::HTTP::Status::METHOD_NOT_ALLOWED);
                return;
            }
        }
    }
}

void DBServerProcessor::get_neighbors() {
    LocalMemory& mem = _threadContext->getLocalMemory();
    const auto& httpInfo = getHttpInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();

    ColumnVector<NodeID>* allNodeIDs = mem.alloc<ColumnVector<NodeID>>();
    ColumnVector<NodeID>* nodeIDs = mem.alloc<ColumnVector<NodeID>>();
    size_t limitPerNode = std::numeric_limits<size_t>::max();

    try {
        const auto json = nlohmann::json::parse(httpInfo._payload);

        // NodeIDs
        const auto nodeIDsIt = json.find("nodeIDs");
        if (nodeIDsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        const auto& vec = nodeIDsIt->get<std::vector<NodeID::Type>>();
        allNodeIDs->reserve(vec.size());
        for (const NodeID::Type nodeID : vec) {
            allNodeIDs->push_back(nodeID);
        }

        if (auto it = json.find("limitPerNode"); it != json.end()) {
            limitPerNode = it->get<size_t>();
        }

    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    payload.key("data");
    payload.obj();

    nodeIDs->resize(1); // One node at a time

    for (auto nodeID : *allNodeIDs) {
        payload.key(nodeID);
        payload.obj();
        payload.key("outs");
        payload.obj();
        payload.key("edges");
        payload.obj();

        bool reachedEnd = true;

        (*nodeIDs)[0] = nodeID;

        size_t j = 0;
        for (const auto& edge : reader.getOutEdges(nodeIDs)) {
            if (j > limitPerNode) {
                reachedEnd = false;
                break;
            }

            j++;

            payload.key(edge._edgeID);
            payload.value<EdgeDir::Out>(edge);
        }

        payload.end(); // edges
        payload.key("reachedEnd");
        payload.value(reachedEnd);
        payload.end(); // outs

        payload.key("ins");
        payload.obj();
        payload.key("edges");
        payload.obj();

        reachedEnd = true;
        (*nodeIDs)[0] = nodeID;

        j = 0;
        for (const auto& edge : reader.getInEdges(nodeIDs)) {
            if (j > limitPerNode) {
                reachedEnd = false;
                break;
            }

            j++;

            payload.key(edge._edgeID);
            payload.value<EdgeDir::In>(edge);
        }

        payload.end(); // edges
        payload.key("reachedEnd");
        payload.value(reachedEnd);
        payload.end(); // ins
        payload.end(); // Node
    }
}

void DBServerProcessor::get_nodes() {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    LocalMemory& mem = _threadContext->getLocalMemory();
    const std::string_view reqBody = parser.getHttpInfo()._payload;

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();
    const auto& metadata = reader.getMetadata();

    payload.setMetadata(&metadata);

    ColumnVector<NodeID>* nodeIDs = mem.alloc<ColumnVector<NodeID>>();

    try {
        const auto json = nlohmann::json::parse(reqBody);

        // Labels
        const auto nodeIDsIt = json.find("nodeIDs");
        if (nodeIDsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        const auto& vec = nodeIDsIt->get<std::vector<NodeID::Type>>();
        nodeIDs->reserve(vec.size());
        for (const NodeID::Type nodeID : vec) {
            nodeIDs->push_back(nodeID);
        }
    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    payload.key("data");
    payload.obj();

    for (const auto nodeID : *nodeIDs) {
        const NodeView node = reader.getNodeView(nodeID);
        payload.key(nodeID);
        payload.value(node);
    }
}

void DBServerProcessor::get_node_edges() {
    LocalMemory& mem = _threadContext->getLocalMemory();
    const std::string_view reqBody = getHttpInfo()._payload;

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();

    payload.setMetadata(&metadata);

    ColumnVector<NodeID>* nodeIDs = mem.alloc<ColumnVector<NodeID>>();
    std::unordered_map<EdgeTypeID, size_t> outEdgeTypeLimits;
    std::unordered_map<EdgeTypeID, size_t> inEdgeTypeLimits;
    size_t defaultLimit = 10;
    bool returnOnlyIDs = false;

    try {
        const auto json = nlohmann::json::parse(reqBody);

        // Labels
        const auto nodeIDsIt = json.find("nodeIDs");
        if (nodeIDsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        const auto& vec = nodeIDsIt->get<std::vector<NodeID::Type>>();
        nodeIDs->reserve(vec.size());
        for (const NodeID::Type nodeID : vec) {
            nodeIDs->push_back(nodeID);
        }

        const auto defaultLimitIt = json.find("defaultLimit");
        if (defaultLimitIt != json.end()) {
            defaultLimit = defaultLimitIt->get<size_t>();
        }

        const auto returnOnlyIDsIt = json.find("returnOnlyIDs");
        if (returnOnlyIDsIt != json.end()) {
            returnOnlyIDs = returnOnlyIDsIt->get<bool>();
        }

        const auto outLimitIt = json.find("limitByOutEdgeType");
        if (outLimitIt != json.end()) {
            for (const auto& item : *outLimitIt) {
                const EdgeTypeID etID = item.at(0).get<EdgeTypeID::Type>();
                if (!etID.isValid()) {
                    continue;
                }

                const size_t edgeLimit = item.at(1).get<size_t>();
                outEdgeTypeLimits[etID] = edgeLimit;
            }
        }

        const auto inLimitIt = json.find("limitByInEdgeType");
        if (inLimitIt != json.end()) {
            for (const auto& item : *inLimitIt) {
                const EdgeTypeID etID = item.at(0).get<EdgeTypeID::Type>();
                if (!etID.isValid()) {
                    continue;
                }

                const size_t edgeLimit = item.at(1).get<size_t>();
                inEdgeTypeLimits[etID] = edgeLimit;
            }
        }

    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    std::unordered_map<EdgeTypeID, size_t> outCounts;
    std::unordered_map<EdgeTypeID, size_t> inCounts;
    payload.key("data");
    payload.obj();

    ColumnVector<NodeID> singleNodeID(1);

    for (const NodeID nodeID : *nodeIDs) {
        outCounts.clear();
        inCounts.clear();
        singleNodeID[0] = nodeID;
        payload.key(nodeID);
        payload.obj();

        const auto outs = reader.getOutEdges(&singleNodeID);
        payload.key("outs");
        payload.arr();
        if (!returnOnlyIDs) {
            for (const EdgeRecord& out : outs) {
                size_t& currentCount = outCounts[out._edgeTypeID];
                const size_t limit = outEdgeTypeLimits.contains(out._edgeTypeID)
                                       ? outEdgeTypeLimits.at(out._edgeTypeID)
                                       : defaultLimit;
                currentCount++;
                if (currentCount > limit) {
                    continue;
                }
                payload.value(reader.getEdgeView(out._edgeID));
            }
        } else {
            for (const EdgeRecord& out : outs) {
                size_t& currentCount = outCounts[out._edgeTypeID];
                const size_t limit = outEdgeTypeLimits.contains(out._edgeTypeID)
                                       ? outEdgeTypeLimits.at(out._edgeTypeID)
                                       : defaultLimit;
                currentCount++;
                if (currentCount > limit) {
                    continue;
                }
                payload.arr();
                payload.value(out._edgeID);
                payload.value(out._otherID);
                payload.end();
            }
        }
        payload.end(); // outs

        const auto ins = reader.getInEdges(&singleNodeID);
        payload.key("ins");
        payload.arr();

        if (!returnOnlyIDs) {
            for (const EdgeRecord& in : ins) {
                size_t& currentCount = inCounts[in._edgeTypeID];
                const size_t limit = inEdgeTypeLimits.contains(in._edgeTypeID)
                                       ? inEdgeTypeLimits.at(in._edgeTypeID)
                                       : defaultLimit;
                currentCount++;
                if (currentCount > limit) {
                    continue;
                }
                payload.value(reader.getEdgeView(in._edgeID));
            }
        } else {
            for (const EdgeRecord& in : ins) {
                size_t& currentCount = inCounts[in._edgeTypeID];
                const size_t limit = inEdgeTypeLimits.contains(in._edgeTypeID)
                                       ? inEdgeTypeLimits.at(in._edgeTypeID)
                                       : defaultLimit;
                currentCount++;
                if (currentCount > limit) {
                    continue;
                }
                payload.arr();
                payload.value(in._edgeID);
                payload.value(in._otherID);
                payload.end();
            }
        }
        payload.end(); // outs

        payload.key("outEdgeCounts");
        payload.obj();
        for (const auto [edgeTypeID, count] : outCounts) {
            payload.key(edgeTypeID);
            payload.value(count);
        }
        payload.end(); // outEdgeCounts

        payload.key("inEdgeCounts");
        payload.obj();
        for (const auto [edgeTypeID, count] : inCounts) {
            payload.key(edgeTypeID);
            payload.value(count);
        }
        payload.end(); // inEdgeCounts

        payload.end(); // node
    }


    payload.end();
}

void DBServerProcessor::explore_node_edges() {
    const auto& httpInfo = getHttpInfo();

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();
    const GraphMetadata& metadata = reader.getMetadata();
    const LabelMap& labels = metadata.labels();
    const EdgeTypeMap& edgeTypes = metadata.edgeTypes();
    const PropertyTypeMap& propTypes = metadata.propTypes();

    payload.setMetadata(&metadata);

    ExploreNodeEdgesExecutor executor {reader, payload};

    try {
        const auto json = nlohmann::json::parse(httpInfo._payload);
        // NodeID
        auto it = json.find("nodeID");
        if (it == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        NodeID nodeID = (size_t)it.value();
        if (!nodeID.isValid()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::INVALID_NODE_ID));
            return;
        }

        executor.setNodeID(nodeID);

        // Labels
        it = json.find("labels");
        if (it != json.end()) {
            for (const auto& label : it.value()) {
                const auto& labelName = label.get<std::string>();
                const auto labelID = labels.get(labelName);
                if (!labelID) {
                    continue;
                }

                executor.addLabel(labelID.value());
            }
        }

        // Skip
        it = json.find("skip");
        if (it != json.end()) {
            executor.setSkip(it.value());
        }

        // Limit
        it = json.find("limit");
        if (it != json.end()) {
            executor.setLimit(it.value());
        }

        // Node Properties
        it = json.find("nodeProperties");
        if (it != json.end()) {
            for (const auto& [type, expr] : it->items()) {
                const auto pType = propTypes.get(type);
                if (!pType) {
                    continue;
                }

                if (pType.value()._valueType != ValueType::String) {
                    continue;
                }

                auto str = expr.get<std::string>();
                std::transform(str.begin(), str.end(), str.begin(), [](char c) { return std::tolower(c); });
                executor.addNodePropertyFilter(pType.value()._id, std::move(str));
            }
        }

        // Edge Properties
        it = json.find("edgeProperties");
        if (it != json.end()) {
            for (const auto& [type, expr] : it->items()) {
                const auto pType = propTypes.get(type);
                if (!pType) {
                    continue;
                }

                if (pType.value()._valueType != ValueType::String) {
                    continue;
                }

                auto str = expr.get<std::string>();
                std::transform(str.begin(), str.end(), str.begin(), [](char c) { return std::tolower(c); });
                executor.addEdgePropertyFilter(pType.value()._id, std::move(str));
            }
        }

        // Edge direction
        it = json.find("excludeOutgoing");
        if (it != json.end()) {
            executor.setExcludeOutgoing(it.value());
        }

        it = json.find("excludeIncoming");
        if (it != json.end()) {
            executor.setExcludeIncoming(it.value());
        }

        // Edge type
        it = json.find("edgeTypes");
        if (it != json.end()) {
            for (const auto& type : it.value()) {
                const auto& etName = type.get<std::string>();
                const auto etID = edgeTypes.get(etName);
                if (!etID) {
                    continue;
                }

                executor.addEdgeType(etID.value());
            }
        }

    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    std::unordered_map<EdgeTypeID, size_t> outCounts;
    std::unordered_map<EdgeTypeID, size_t> inCounts;
    payload.key("data");

    executor.run();
}

void DBServerProcessor::get_edges() {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    LocalMemory& mem = _threadContext->getLocalMemory();
    const std::string_view reqBody = parser.getHttpInfo()._payload;

    const auto header = _writer.startHeader(net::HTTP::Status::OK,
                                            !_connection.isCloseRequired());

    PayloadWriter payload(_writer.getWriter());
    payload.obj();

    const auto info = getTransactionInfo();
    const auto transaction = _db.getSystemManager().openTransaction(info.graphName,
                                                                    info.commit,
                                                                    info.change);

    if (!transaction) {
        auto txError = transaction.error().fmtMessage();
        payload.key("error");
        payload.value(txError);
        return;
    }

    const auto reader = transaction.value().readGraph();
    payload.setMetadata(&reader.getMetadata());

    ColumnVector<EdgeID>* edgeIDs = mem.alloc<ColumnVector<EdgeID>>();

    try {
        const auto json = nlohmann::json::parse(reqBody);

        const auto edgeIDsIt = json.find("edgeIDs");
        if (edgeIDsIt == json.end()) {
            payload.key("error");
            payload.value(EndpointStatusDescription::value(EndpointStatus::MISSING_PARAM));
            return;
        }

        const auto& vec = edgeIDsIt->get<std::vector<EdgeID::Type>>();
        edgeIDs->reserve(vec.size());
        for (const EdgeID::Type edgeID : vec) {
            edgeIDs->push_back(edgeID);
        }
    } catch (const std::exception& e) {
        payload.key("error");
        payload.value(EndpointStatusDescription::value(EndpointStatus::COULD_NOT_PARSE_BODY));
        return;
    }

    payload.key("data");
    payload.obj();

    for (const auto edgeID : *edgeIDs) {
        const EdgeView edge = reader.getEdgeView(edgeID);
        payload.key(edgeID);
        payload.value(edge);
    }
}

DBServerProcessor::TransactionInfo DBServerProcessor::getTransactionInfo() const {
    auto& parser = _connection.getParser<net::HTTPParser<DBURIParser>>();
    const auto& httpInfo = parser.getHttpInfo();
    std::string_view graphNameView = httpInfo._params[(size_t)DBHTTPParams::graph];
    std::string_view commitHashStr = httpInfo._params[(size_t)DBHTTPParams::commit];
    std::string_view changeHashStr = httpInfo._params[(size_t)DBHTTPParams::change];

    if (graphNameView.empty()) {
        graphNameView = "default";
    }

    if (commitHashStr.empty()) {
        commitHashStr = "head";
    }

    if (changeHashStr.empty()) {
        changeHashStr = "head";
    }

    auto commitHashRes = CommitHash::fromString(commitHashStr);

    if (!commitHashRes) {
        return {
            .graphName = std::string {graphNameView},
            .commit = CommitHash::head(),
            .change = ChangeID::head(),
        };
    }

    auto changeHashRes = ChangeID::fromString(changeHashStr);

    if (!changeHashRes) {
        return {
            .graphName = std::string {graphNameView},
            .commit = commitHashRes.value(),
            .change = ChangeID::head(),
        };
    }

    return {
        .graphName = std::string {graphNameView},
        .commit = commitHashRes.value(),
        .change = changeHashRes.value(),
    };
}
