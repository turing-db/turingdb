#pragma once

#include <string_view>

namespace db::v2 {

class CypherAST;

class Symbol {
public:
    friend CypherAST;

    static Symbol* create(CypherAST* ast, std::string_view name);

    std::string_view getName() const { return _name; }

private:
    std::string_view _name;

    Symbol(std::string_view name)
        : _name(name)
    {
    }

    ~Symbol() = default;
};

}
