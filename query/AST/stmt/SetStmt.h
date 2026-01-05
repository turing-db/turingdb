#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class Pattern;
class CypherAST;
class SetItem;

class SetStmt : public Stmt {
public:
    using SetItems = std::vector<SetItem*>;

    static SetStmt* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SET; }

    void addItem(SetItem* item) { _items.push_back(item); }

    const SetItems& getItems() const { return _items; }

private:
    SetItems _items;

    ~SetStmt() override;
};

}
