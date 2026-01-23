#pragma once

#include <stddef.h>

namespace db::json::neo4j {

class Neo4JParserConfig {
public:
    static constexpr std::size_t nodeCountLimit = 400000;
    static constexpr std::size_t edgeCountLimit = 1000000;

    Neo4JParserConfig() = delete;
    ~Neo4JParserConfig() = delete;
};

}
