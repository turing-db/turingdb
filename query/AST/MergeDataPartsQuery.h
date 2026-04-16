#pragma once

#include "QueryCommand.h"

namespace db {

class CypherAST;
class DeclContext;

class MergeDataPartsQuery : public QueryCommand {
public:
    static MergeDataPartsQuery* create(CypherAST* ast);

    Kind getKind() const override { return Kind::MERGE_DATAPARTS_QUERY; }

private:
    MergeDataPartsQuery(DeclContext* declContext);
    ~MergeDataPartsQuery() override;
};

}
