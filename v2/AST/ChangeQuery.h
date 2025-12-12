#pragma once

#include <stdint.h>

#include "QueryCommand.h"

namespace db::v2 {

class CypherAST;
class StmtContainer;
class ReturnStmt;
class DeclContext;

class ChangeQuery : public QueryCommand {
public:
    enum class Op : uint8_t {
        NEW = 0,
        SUBMIT,
        DELETE,
        LIST,
    };

    static ChangeQuery* create(CypherAST* ast, Op op);

    Kind getKind() const override { return Kind::CHANGE_QUERY; }
    Op getOp() const { return _op; }

private:
    ChangeQuery(DeclContext* declContext, Op op);
    ~ChangeQuery() override;

    Op _op {};
};

}
