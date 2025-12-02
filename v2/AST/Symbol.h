#pragma once

#include <string_view>

namespace db::v2 {

class CypherAST;

class Symbol {
public:
    friend CypherAST;

    static Symbol* create(CypherAST* ast, std::string_view name);

    std::string_view getName() const { return _name; }
    std::string_view getOriginalName() const { return _originalName; }

    void setAs(std::string_view name) {
        _name = name;
    }

private:
    std::string_view _name;
    std::string_view _originalName;

    Symbol(std::string_view name)
        : _name(name),
        _originalName(name)
    {
    }

    ~Symbol() = default;
};

}
