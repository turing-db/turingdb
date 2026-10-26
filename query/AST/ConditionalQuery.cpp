#include "ConditionalQuery.h"

#include <algorithm>

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

ConditionalQuery::ConditionalQuery(DeclContext* declContext, const Branches& branches)
    : RegularQuery(declContext),
    _branches(branches)
{
}

ConditionalQuery::~ConditionalQuery() {
}

ConditionalQuery* ConditionalQuery::create(CypherAST* ast, const Branches& branches) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    ConditionalQuery* query = new ConditionalQuery(declContext, branches);

    ast->addQuery(query);

    for (const Branch& branch : branches) {
        ast->nestQuery(branch._query);
    }

    return query;
}

bool ConditionalQuery::writesToTheGraph() const {
    return std::ranges::any_of(_branches, [](const Branch& branch) {
        return branch._query->writesToTheGraph();
    });
}

bool ConditionalQuery::readsTheGraph() const {
    return std::ranges::any_of(_branches, [](const Branch& branch) {
        return branch._query->readsTheGraph();
    });
}
