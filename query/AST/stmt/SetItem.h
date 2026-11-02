#pragma once

#include <variant>
#include <vector>

#include "decl/PatternData.h"

namespace db {

class CypherAST;
class PropertyExpr;
class Expr;
class SymbolExpr;
class EntityTypeExpr;

class SetItem {
public:
    struct PropertyExprAssign {
        PropertyExprAssign() {}
        PropertyExprAssign(PropertyExpr* type, Expr* val)
            : _propTypeExpr(type), _propValueExpr(val)
        {
        }
        PropertyExpr* _propTypeExpr {nullptr};
        Expr* _propValueExpr {nullptr};
    };

    struct EntityPropertiesAssign {
        SymbolExpr* _entity {nullptr};
        Expr* _value {nullptr};
        std::vector<EntityPropertyConstraint> _createdProperties;
    };

    struct SymbolEntityTypes {
        EntityTypeExpr* _value {nullptr};
    };

    using Variant = std::variant<PropertyExprAssign,
                                 EntityPropertiesAssign,
                                 SymbolEntityTypes>;

    static SetItem* create(CypherAST* ast, PropertyExpr* expr, Expr* value);
    static SetItem* create(CypherAST* ast, EntityTypeExpr* value);
    static SetItem* create(CypherAST* ast, SymbolExpr* entity, Expr* value);

    Variant& item() { return _item; }
    const Variant& item() const { return _item; }

private:
    friend CypherAST;

    Variant _item;

    SetItem();
    ~SetItem();
};

}
