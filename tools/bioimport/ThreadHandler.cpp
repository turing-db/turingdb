#include "ThreadHandler.h"
#include "JsonParser.h"

bool ThreadHandler::start(JsonParser& parser) {
    // Retrieving the node and edge counts
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
        requests.nodes.emplace_back(Neo4JHttpRequest::RequestProps{
            .statement = "MATCH(n) "
                         "RETURN labels(n), ID(n), properties(n) SKIP " +
                         std::to_string(nodeRequested) + " LIMIT " +
                         std::to_string(_nodeCountLimit) + ";",
            .silent = true,
        });

        nodeRequested += _nodeCountLimit;
    }

    uint64_t edgeRequested = 0;

    // Preparing node requests
    while (edgeRequested < stats.edgeCount) {
        requests.edges.emplace_back(Neo4JHttpRequest::RequestProps{
            .statement = "MATCH (n1)-[e]->(n2) "
                         "RETURN type(e), ID(n1), ID(n2), properties(e) SKIP " +
                         std::to_string(edgeRequested) + " LIMIT " +
                         std::to_string(_edgeCountLimit) + ";",
            .silent = true,
        });

        edgeRequested += _edgeCountLimit;
    }

    _thread = std::thread(&ThreadHandler::exec, this);
    return true;
}

void ThreadHandler::exec() {
    requests.nodeProperties.exec();
    if (!requests.nodeProperties.success()) {
        return;
    }

    for (auto& nodeRequest : requests.nodes) {
        nodeRequest.exec();
        if (!nodeRequest.success()) {
            return;
        }
    }

    requests.edgeProperties.exec();
    if (!requests.edgeProperties.success()) {
        return;
    }

    for (auto& edgeRequest : requests.edges) {
        edgeRequest.exec();
        if (!edgeRequest.success()) {
            return;
        }
    }
}
