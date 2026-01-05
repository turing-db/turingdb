#pragma once

#include "Stmt.h"

namespace db {

class Projection;
class CypherAST;

class ReturnStmt : public Stmt {
public:
    static ReturnStmt* create(CypherAST* ast, Projection* projection);

    Kind getKind() const override { return Kind::RETURN; }

    Projection* getProjection() const { return _projection; }

private:
    Projection* _projection {nullptr};

    ReturnStmt(Projection* projection);
    ~ReturnStmt() override;
};

}
