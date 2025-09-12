#pragma once

#include "Stmt.h"

namespace db::v2 {

class Projection;
class CypherAST;

class ReturnStmt : public Stmt {
public:

    static ReturnStmt* create(CypherAST* ast, Projection* projection);

    Projection* getProjection() const { return _projection; }

private:
    Projection* _projection {nullptr};

    ReturnStmt(Projection* projection);
    ~ReturnStmt() override;
};

}
