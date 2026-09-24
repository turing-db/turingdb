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
