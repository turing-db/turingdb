#pragma once

#include <variant>

namespace db {

class CypherAST;
class PropertyExpr;
class Expr;
class Symbol;
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

    struct SymbolAddAssign {
        Symbol* _symbol {nullptr};
        Expr* _value {nullptr};
    };

    struct SymbolEntityTypes {
        EntityTypeExpr* _value {nullptr};
    };

    using Variant = std::variant<PropertyExprAssign,
                                 SymbolAddAssign,
                                 SymbolEntityTypes>;

    static SetItem* create(CypherAST* ast, PropertyExpr* expr, Expr* value);
    static SetItem* create(CypherAST* ast, EntityTypeExpr* value);
    static SetItem* create(CypherAST* ast, Symbol* symbol, Expr* value);

    Variant& item() { return _item; }
    const Variant& item() const { return _item; }

private:
    friend CypherAST;

    Variant _item;

    SetItem();
    ~SetItem();
};

}
