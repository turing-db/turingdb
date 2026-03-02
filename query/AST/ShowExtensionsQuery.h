#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class ShowExtensionsQuery : public QueryCommand {
public:
    static ShowExtensionsQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::SHOW_EXTENSIONS_QUERY; }

private:
    ShowExtensionsQuery(DeclContext* declContext);
    ~ShowExtensionsQuery() override;
};

}
