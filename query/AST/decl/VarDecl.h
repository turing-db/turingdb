#pragma once

#include <string_view>

#include "decl/ExprType.h"

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
    void setType(EvaluatedType type) { _exprType._type = type; }

    void setListShape(const ListShape& shape) { _exprType._shape = shape; }

    void setCommonType(EvaluatedType type) { _exprType._commonType = type; }

    void setExprType(const ExprType& type) { _exprType = type; }

    // The variable of the enclosing query an EXISTS or COUNT body imported this one from;
    // null for a variable the body declares itself, even under the name of an outer one
    void setImportSource(const VarDecl* source) { _importSource = source; }
    const VarDecl* getImportSource() const { return _importSource; }

    EvaluatedType getType() const { return _exprType._type; }

    EvaluatedTypeSet getCandidateTypes() const { return _exprType._candidates; }

    // How deeply a List-typed variable nests and what its innermost elements are. A
    // barrier publishes it beside the type, so an UNWIND behind the barrier binds its
    // variable to the elements' own type rather than to tagged scalars.
    const ListShape& getListShape() const { return _exprType._shape; }

    const ExprType& getExprType() const { return _exprType; }

    // The type every value converts to without loss: the type itself, or for Any the one
    // the selection it was published from shares, Any if none
    EvaluatedType getCommonType() const { return _exprType._type == EvaluatedType::Any ? _exprType._commonType : _exprType._type; }

    const std::string_view& getName() const { return _name; }
    bool isUnnamed() const { return _isUnnamed; }
    bool isUnwound() const { return _isUnwound; }

    // A variable of a variable-length pattern binds the whole path - a list, not one
    // entity - so no property or label is read off it
    bool isQuantifiedPath() const { return _isQuantifiedPath; }

private:
    ExprType _exprType;
    std::string_view _name;
    const VarDecl* _importSource {nullptr};
    bool _isUnnamed {false};
    bool _isUnwound {false};
    bool _isQuantifiedPath {false};

    VarDecl(EvaluatedType type, std::string_view name)
        : _exprType {type, {}, {}},
        _name(name)
    {
    }

    ~VarDecl() = default;
};

}
