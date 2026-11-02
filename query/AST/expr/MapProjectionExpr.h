#pragma once

#include <stdint.h>
#include <vector>

#include "Expr.h"

namespace db {

class CypherAST;
class Symbol;
class SymbolExpr;

// A Cypher map projection: `v {.name, .*, rank: v.age + 1, x}` over a variable bound to a
// node, an edge or a map. A later entry replaces an earlier one under the same key, every
// entry wins over the properties `.*` reads wherever it stands, and a null base projects to null.
class MapProjectionExpr : public Expr {
public:
    enum class ItemKind : uint8_t {
        Property,
        AllProperties,
        Literal,
        Variable,
    };

    // `.key` has a key and, once analyzed, the read of the base's property under it as its
    // value. `.*` has neither. `key: expr` has both, and `x` has x's name as its key and the
    // variable as its value.
    struct Item {
        ItemKind _kind {ItemKind::Property};
        Symbol* _key {nullptr};
        Expr* _value {nullptr};
    };

    using Items = std::vector<Item>;

    SymbolExpr* getBase() const { return _base; }

    const Items& getItems() const { return _items; }
    Items& getItems() { return _items; }

    static MapProjectionExpr* create(CypherAST* ast, SymbolExpr* base, const Items& items);

private:
    SymbolExpr* _base {nullptr};
    Items _items;

    MapProjectionExpr(SymbolExpr* base, const Items& items);
    ~MapProjectionExpr() override;
};

}
