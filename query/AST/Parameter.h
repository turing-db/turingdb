#pragma once

#include <string_view>

namespace db {

class CypherAST;

class Parameter {
public:
    friend CypherAST;

    static Parameter* create(CypherAST* ast, std::string_view name);

    std::string_view getName() const { return _name; }

private:
    std::string_view _name;

    Parameter(std::string_view name);
    ~Parameter();
};

}
