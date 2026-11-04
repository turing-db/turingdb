#pragma once

#include <stddef.h>
#include <string_view>

namespace db {

class CypherAST;

class CypherParser {
public:
    // The analyzer and codegen recurse once per level of an expression, about 800 bytes of
    // stack each, so this many levels must fit the 512 KB a macOS thread starts with
    static constexpr size_t MAX_EXPRESSION_DEPTH = 256;

    // The parser recurses once per level of (, [, { or CASE, so a query nested deeper is
    // turned away before it can exhaust the stack
    static constexpr size_t MAX_NESTING_DEPTH = 256;

    // The frames between two levels depend on the clauses between them, so a level also stops
    // the parse past this much stack, leaving a 512 KB macOS thread room for its callers and the throw
    static constexpr size_t MAX_STACK_BYTES = 384 * 1024;

    explicit CypherParser(CypherAST* ast);
    ~CypherParser() = default;

    CypherParser(const CypherParser&) = delete;
    CypherParser(CypherParser&&) = default;
    CypherParser& operator=(const CypherParser&) = delete;
    CypherParser& operator=(CypherParser&&) = default;

    void parse(std::string_view query);

    CypherAST* getAST() const { return _ast; }

private:
    CypherAST* _ast {nullptr};
};

}
