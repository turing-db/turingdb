#include "Neo4JQueryManager.h"

#include "Neo4JHttpRequest.h"

using namespace db;

bool Neo4JQueryManager::isServerRunning() const {
    std::string response;
    return Neo4JHttpRequest::execStatic(&response,
                                       _url, _urlSuffix,
                                       _username, _password,
                                       _port, "GET", "");
}

Neo4JQueryManager::Query Neo4JQueryManager::dbStatsQuery() const {
    auto query = std::make_unique<Neo4JHttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("CALL {MATCH (n) RETURN count(n) as nodeCount} "
                        "CALL {MATCH ()-[r]->() RETURN count(r) as edgeCount} "
                        "RETURN nodeCount, edgeCount");

    return Query{std::move(query)};
}

Neo4JQueryManager::Query Neo4JQueryManager::nodeLabelsQuery() const {
    auto query = std::make_unique<Neo4JHttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("CALL db.labels()");

    return Query {std::move(query)};
}

QueryManager::Query QueryManager::nodeLabelSetsQuery() const {
    auto query = std::make_unique<HttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("MATCH (n) "
                        "WITH labels(n) as labelSet "
                        "RETURN DISTINCT labelSet");

    return Query {std::move(query)};
}

Neo4JQueryManager::Query Neo4JQueryManager::edgeTypesQuery() const {
    auto query = std::make_unique<Neo4JHttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("CALL db.relationshipTypes()");

    return Query{std::move(query)};
}

Neo4JQueryManager::Query Neo4JQueryManager::nodePropertiesQuery() const {
    auto query = std::make_unique<Neo4JHttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("CALL db.schema.nodeTypeProperties()");

    return Query{std::move(query)};
}

Neo4JQueryManager::Query Neo4JQueryManager::edgePropertiesQuery() const {
    auto query = std::make_unique<Neo4JHttpRequest>();
    query->setUrl(_url);
    query->setUrlSuffix(_urlSuffix);
    query->setUsername(_username);
    query->setPassword(_password);
    query->setPort(_port);
    query->setMethod("POST");
    query->setStatement("CALL db.schema.relTypeProperties()");

    return Query{std::move(query)};
}

std::vector<Neo4JQueryManager::Query> Neo4JQueryManager::nodesQueries(size_t nodeCount,
                                                                     size_t countPerQuery) const {
    std::vector<Query> queries;
    uint64_t requested = 0;

    while (requested < nodeCount) {
        auto& query = queries.emplace_back();
        query._request = std::make_unique<Neo4JHttpRequest>();
        query._request->setUrl(_url);
        query._request->setUrlSuffix(_urlSuffix);
        query._request->setUsername(_username);
        query._request->setPassword(_password);
        query._request->setPort(_port);
        query._request->setMethod("POST");
        query._request->setStatement("MATCH(n) "
                                    "RETURN labels(n), ID(n), properties(n) "
                                    "SKIP "
                                    + std::to_string(requested) + " LIMIT "
                                    + std::to_string(countPerQuery));
        requested += countPerQuery;
    }

    return queries;
}

std::vector<Neo4JQueryManager::Query> Neo4JQueryManager::edgesQueries(size_t edgeCount,
                                                                     size_t countPerQuery) const {
    std::vector<Query> queries;
    uint64_t requested = 0;

    while (requested < edgeCount) {
        auto& query = queries.emplace_back();
        query._request = std::make_unique<Neo4JHttpRequest>();
        query._request->setUrl(_url);
        query._request->setUrlSuffix(_urlSuffix);
        query._request->setUsername(_username);
        query._request->setPassword(_password);
        query._request->setPort(_port);
        query._request->setMethod("POST");
        query._request->setStatement("MATCH (n1)-[e]->(n2) "
                                    "RETURN type(e), ID(n1), ID(n2), properties(e) "
                                    "SKIP "
                                    + std::to_string(requested) + " LIMIT "
                                    + std::to_string(countPerQuery));
        requested += countPerQuery;
    }

    return queries;
}