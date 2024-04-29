#pragma once

#include "Neo4JHttpRequest.h"

#include <thread>
#include <vector>

class JsonParser;

class ThreadHandler {
public:
    struct Requests {
        Neo4JHttpRequest stats {
            "CALL {MATCH (n) RETURN count(n) as nodeCount} "
            "CALL {MATCH ()-[r]->() RETURN count(r) as edgeCount} "
            "RETURN nodeCount, edgeCount",
        };

        Neo4JHttpRequest nodeProperties {
            "CALL db.schema.nodeTypeProperties()",
        };

        std::vector<Neo4JHttpRequest> nodes;

        Neo4JHttpRequest edgeProperties {
            "CALL db.schema.relTypeProperties();",
        };

        std::vector<Neo4JHttpRequest> edges;
    } requests;

    void initialize();
    void setUrl(const std::string& url) { _url = url; }
    void setUrlSuffix(const std::string& suffix) { _urlSuffix = suffix; }
    void setUsername(const std::string& username) { _username = username; }
    void setPassword(const std::string& password) { _password = password; }
    void setPort(uint64_t port) { _port = port; }
    bool start(JsonParser& parser);
    void join() { _thread.join(); }

private:
    std::thread _thread;
    std::string _url = "localhost";
    std::string _urlSuffix = "/db/data/transaction/commit";
    uint64_t _port = 7474;
    std::string _username = "neo4j";
    std::string _password = "turing";
    static inline constexpr size_t _nodeCountLimit = 1000000;
    static inline constexpr size_t _edgeCountLimit = 3000000;

    void exec();
    void setRequestParams(Neo4JHttpRequest& req);
};
