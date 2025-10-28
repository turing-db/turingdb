#pragma once

#include <variant>

namespace db::v2 {

class CypherAST;
class PropertyExpr;
class Expr;
class Symbol;
class EntityTypeExpr;

class SetItem {
public:
    struct PropertyExprAssign {
        static constexpr std::size_t index = 0;

        PropertyExpr* propTypeExpr;
        Expr* propValueExpr;
    };

    struct SymbolAddAssign {
        static constexpr std::size_t index = 1;

        Symbol* symbol {nullptr};
        Expr* value {nullptr};
    };

    struct SymbolEntityTypes {
        static constexpr std::size_t index = 2;

        EntityTypeExpr* value {nullptr};
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
