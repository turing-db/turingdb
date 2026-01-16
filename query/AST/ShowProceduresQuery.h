#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class ShowProceduresQuery : public QueryCommand {
public:
    static ShowProceduresQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SHOW_PROCEDURES_QUERY; }

private:
    ShowProceduresQuery(DeclContext* declContext);
    ~ShowProceduresQuery() override;
};

}
