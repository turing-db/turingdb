#pragma once

#include <array>
#include <cstdint>
#include <string_view>

namespace db::json::neo4j {

enum class ParserMessageID : uint64_t {
    CreatedPTypeDuringNodeParsing = 0,
};

constexpr std::array PARSER_MESSAGES {
    "Created \"PType %s\" during the parsing of nodes",
};

}
