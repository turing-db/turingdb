#pragma once

#include <stdint.h>

#include "QueryCommand.h"
#include "ChangeOp.h"

namespace db::v2 {

class CypherAST;
class StmtContainer;
class ReturnStmt;
class DeclContext;

class ChangeQuery : public QueryCommand {
public:
    static ChangeQuery* create(CypherAST* ast, ChangeOp op);

    Kind getKind() const override { return Kind::CHANGE_QUERY; }

    ChangeOp getOp() const { return _op; }

private:
    ChangeOp _op;

    ChangeQuery(DeclContext* declContext, ChangeOp op);
    ~ChangeQuery() override;
};

}
