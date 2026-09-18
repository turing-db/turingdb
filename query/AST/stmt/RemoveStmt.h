#pragma once

#include <vector>

#include "Stmt.h"

namespace db {

class CypherAST;
class PropertyExpr;

class RemoveStmt : public Stmt {
public:
    using Properties = std::vector<PropertyExpr*>;

    static RemoveStmt* create(CypherAST* ast);

    Kind getKind() const override { return Kind::REMOVE; }

    void addProperty(PropertyExpr* property) { _properties.push_back(property); }

    const Properties& getProperties() const { return _properties; }

private:
    Properties _properties;

    ~RemoveStmt() override;
};

}
