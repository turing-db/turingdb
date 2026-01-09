#pragma once

#include <string_view>

#include "decl/EvaluatedType.h"

namespace db {

class CypherAST;
class DeclContext;

class VarDecl {
public:
    friend CypherAST;

    static VarDecl* create(CypherAST* ast,
                           DeclContext* declContext,
                           std::string_view name,
                           EvaluatedType type);

    void setIsUnnamed(bool isUnnamed) { _isUnnamed = isUnnamed; }

    EvaluatedType getType() const { return _type; }
    const std::string_view& getName() const { return _name; }
    bool isUnnamed() const { return _isUnnamed; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
    bool _isUnnamed {false};

    VarDecl(EvaluatedType type, std::string_view name)
        : _type(type),
        _name(name)
    {
    }

    ~VarDecl() = default;
};

}
