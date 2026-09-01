#pragma once

#include <string_view>

#include "decl/EvaluatedType.h"
#include "decl/ListShape.h"

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

    void setListShape(const ListShape& shape) { _listShape = shape; }

    EvaluatedType getType() const { return _type; }

    // How deeply a List-typed variable nests and what its innermost elements are. A
    // barrier publishes it beside the type, so an UNWIND behind the barrier binds its
    // variable to the elements' own type rather than to tagged scalars.
    const ListShape& getListShape() const { return _listShape; }

    const std::string_view& getName() const { return _name; }
    bool isUnnamed() const { return _isUnnamed; }
    bool isUnwound() const { return _isUnwound; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _listShape;
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
