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
    void setIsUnwound(bool isUnwound) { _isUnwound = isUnwound; }
    void setType(EvaluatedType type) { _type = type; }

    EvaluatedType getType() const { return _type; }
    const std::string_view& getName() const { return _name; }
    bool isUnnamed() const { return _isUnnamed; }
    bool isUnwound() const { return _isUnwound; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    std::string_view _name;
    bool _isUnnamed {false};
    bool _isUnwound {false};

    VarDecl(EvaluatedType type, std::string_view name)
        : _type(type),
        _name(name)
    {
    }

    ~VarDecl() = default;
};

}
