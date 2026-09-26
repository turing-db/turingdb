#pragma once

#include <string_view>
#include <variant>
#include <vector>

namespace db {

class CypherAST;
class PropertyExpr;
class Expr;
class Symbol;
class EntityTypeExpr;
class VarDecl;

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

    // SET n += map writes the map's entries and SET n = map leaves the entity holding them
    // alone. The analyzer spells each entry out as the property write it is, and names the
    // properties the entity may hold that a replacement removes.
    struct SymbolMapAssign {
        Symbol* _symbol {nullptr};
        Expr* _value {nullptr};
        bool _replaces {false};
        VarDecl* _decl {nullptr};
        std::vector<PropertyExprAssign> _entries;
        std::vector<std::string_view> _removedProperties;

        // The value is a node or an edge, read property by property: an addition writes
        // only the properties it holds
        bool _copiesEntity {false};

        // What the property reads resolve against when the value is an expression rather
        // than a variable naming the entity
        VarDecl* _sourceDecl {nullptr};

        // The value is a map computed at run time, whose keys only its rows know: the entries
        // are written as each row names them, and there is nothing to spell out
        bool _writesRowEntries {false};
    };

    struct SymbolEntityTypes {
        EntityTypeExpr* _value {nullptr};
    };

    using Variant = std::variant<PropertyExprAssign,
                                 SymbolMapAssign,
                                 SymbolEntityTypes>;

    static SetItem* create(CypherAST* ast, PropertyExpr* expr, Expr* value);
    static SetItem* create(CypherAST* ast, EntityTypeExpr* value);
    static SetItem* create(CypherAST* ast, Symbol* symbol, Expr* value, bool replaces);

    Variant& item() { return _item; }
    const Variant& item() const { return _item; }

private:
    friend CypherAST;

    Variant _item;

    SetItem();
    ~SetItem();
};

}
