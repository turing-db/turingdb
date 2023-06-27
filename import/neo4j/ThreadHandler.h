#pragma once

#include "Neo4JHttpRequest.h"

#include <vector>
#include <thread>

class JsonParser;

class ThreadHandler {
public:
    struct Requests {
        Neo4JHttpRequest stats{
            "CALL apoc.meta.stats() "
            "YIELD nodeCount, relCount RETURN *",
        };

        Neo4JHttpRequest nodeProperties{
            "CALL apoc.meta.nodeTypeProperties()",
        };

        std::vector<Neo4JHttpRequest> nodes;

        Neo4JHttpRequest edgeProperties{
            "CALL apoc.meta.relTypeProperties();",
        };

        std::vector<Neo4JHttpRequest> edges;
    } requests;

    bool start(JsonParser& parser);
    void join() { _thread.join(); }

private:
    std::thread _thread;
    static inline constexpr size_t _nodeCountLimit = 1000000;
    static inline constexpr size_t _edgeCountLimit = 3000000;

    void exec();
};

