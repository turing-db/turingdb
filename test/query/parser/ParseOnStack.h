#pragma once

#include <stddef.h>
#include <string>
#include <string_view>

namespace turing::test {

struct Nesting {
    std::string_view _prefix;
    std::string_view _open;
    std::string_view _core;
    std::string_view _close;
    size_t _openingOffset {0};
    size_t _openingLength {1};
};

// The prefix, then @param levels openings around the core, then as many closings
void buildNestedQuery(const Nesting& nesting, size_t levels, std::string& query);

// Parses @param query on a thread of @param stackBytes; @param error gets what the parse threw
void parseOnStack(const std::string& query, size_t stackBytes, std::string& error);

}
