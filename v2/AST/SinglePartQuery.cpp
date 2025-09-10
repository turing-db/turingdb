#include "SinglePartQuery.h"

#include "CypherAST.h"

using namespace db::v2;

SinglePartQuery::SinglePartQuery()
{
}

SinglePartQuery::~SinglePartQuery() {
}

SinglePartQuery* SinglePartQuery::create(CypherAST* ast) {
    SinglePartQuery* query = new SinglePartQuery();
    ast->addQuery(query);
    return query;
}
