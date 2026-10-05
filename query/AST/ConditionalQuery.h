#pragma once

#include "QueryCommand.h"

namespace db {

class CallSubqueryStmt;
class CypherAST;

// A standalone WHEN ... THEN ... ELSE query. It is the body of a CALL () { ... } with
// nothing around it, so it holds that subquery and its result is what the subquery returns.
class ConditionalQuery : public QueryCommand {
public:
    static ConditionalQuery* create(CypherAST* ast, CallSubqueryStmt* body);

    Kind getKind() const override { return Kind::CONDITIONAL_QUERY; }

    CallSubqueryStmt* getBody() const { return _body; }

private:
    CallSubqueryStmt* _body {nullptr};

    ConditionalQuery(DeclContext* declContext, CallSubqueryStmt* body);
    ~ConditionalQuery() override;
};

}
