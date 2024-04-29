#include "ThreadHandler.h"
#include "JsonParser.h"

void ThreadHandler::initialize() {
    if (_urlSuffix.empty()) {
        _urlSuffix = "/db/data/transaction/commit";
    }

    if (_port == 0) {
        _port = 7474;
    }

    if (_username.empty()) {
        _username = "neo4j";
    }

    if (_password.empty()) {
        _password = "turing";
    }
}

bool ThreadHandler::start(JsonParser& parser) {
    // Retrieving the node and edge counts
    setRequestParams(requests.stats);
    requests.stats.exec();
    if (!requests.stats.success()) {
        requests.stats.reportError();
        return false;
    }

    if (!parser.parse(requests.stats.getData(),
                      JsonParser::FileFormat::Neo4j4_Stats)) {
        return false;
    }

    const auto& stats = parser.getStats();
    uint64_t nodeRequested = 0;

    // Preparing node requests
    while (nodeRequested < stats.nodeCount) {
        requests.nodes.emplace_back(
            "MATCH(n) "
            "RETURN labels(n), ID(n), properties(n) SKIP "
            + std::to_string(nodeRequested) + " LIMIT " + std::to_string(_nodeCountLimit) + ";");

        nodeRequested += _nodeCountLimit;
    }

    uint64_t edgeRequested = 0;

    // Preparing node requests
    while (edgeRequested < stats.edgeCount) {
        requests.edges.emplace_back(
            "MATCH (n1)-[e]->(n2) "
            "RETURN type(e), ID(n1), ID(n2), properties(e) SKIP "
            + std::to_string(edgeRequested) + " LIMIT " + std::to_string(_edgeCountLimit) + ";");

        edgeRequested += _edgeCountLimit;
    }

    _thread = std::thread(&ThreadHandler::exec, this);
    return true;
}

void ThreadHandler::exec() {
    setRequestParams(requests.nodeProperties);
    requests.nodeProperties.exec();
    if (!requests.nodeProperties.success()) {
        return;
    }

    for (auto& nodeRequest : requests.nodes) {
        setRequestParams(nodeRequest);
        nodeRequest.exec();
        if (!nodeRequest.success()) {
            return;
        }
    }

    setRequestParams(requests.edgeProperties);
    requests.edgeProperties.exec();
    if (!requests.edgeProperties.success()) {
        return;
    }

    for (auto& edgeRequest : requests.edges) {
        setRequestParams(edgeRequest);
        edgeRequest.exec();
        if (!edgeRequest.success()) {
            return;
        }
    }
}

void ThreadHandler::setRequestParams(Neo4JHttpRequest& req) {
    req.setUrl(_url);
    req.setUrlSuffix(_urlSuffix);
    req.setUsername(_username);
    req.setPassword(_password);
    req.setPort(_port);
}
