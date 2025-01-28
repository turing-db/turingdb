#pragma once

#include <string>
#include <unordered_map>

#include "ParserMessages.h"

namespace db::json::neo4j {

class Parser {
protected:
    std::unordered_map<std::string, size_t> _msgCounts;

    template <typename...Args>
    void recordMessage(ParserMessageID id, Args&&... args) {
        const auto* raw_msg = PARSER_MESSAGES[(size_t)id];
        size_t size = std::snprintf(nullptr, 0, raw_msg, args...) + 1;
        std::string msg(size, ' ');
        std::snprintf(msg.data(), msg.size(), raw_msg, args...);
        _msgCounts[msg]++;
    }
};

}
