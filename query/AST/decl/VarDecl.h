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
    void setIsQuantifiedPath(bool isQuantifiedPath) { _isQuantifiedPath = isQuantifiedPath; }
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

    // A variable of a variable-length pattern binds the whole path - a list, not one
    // entity - so no property or label is read off it
    bool isQuantifiedPath() const { return _isQuantifiedPath; }

private:
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _listShape;
    std::string_view _name;
    bool _isUnnamed {false};
    bool _isUnwound {false};
    bool _isQuantifiedPath {false};

    VarDecl(EvaluatedType type, std::string_view name)
        : _type(type),
        _name(name)
    {
    }

    ~VarDecl() = default;
};

}
