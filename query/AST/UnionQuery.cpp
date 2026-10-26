#include "UnionQuery.h"

#include <algorithm>

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db;

UnionQuery::UnionQuery(DeclContext* declContext)
    : RegularQuery(declContext)
{
}

UnionQuery::~UnionQuery() {
}

UnionQuery* UnionQuery::create(CypherAST* ast, RegularQuery* first, const Branches& rest) {
    DeclContext* declContext = DeclContext::create(ast, nullptr);
    UnionQuery* query = new UnionQuery(declContext);

    query->_branches.push_back({first, false});
    for (const Branch& branch : rest) {
        query->_branches.push_back(branch);
    }

    ast->addQuery(query);

    for (const Branch& branch : query->_branches) {
        ast->nestQuery(branch._query);
    }

    return query;
}

size_t UnionQuery::getDedupedBranchCount() const {
    size_t deduped = 0;

    for (size_t index = 1; index < _branches.size(); index++) {
        if (!_branches[index]._all) {
            deduped = index + 1;
        }
    }

    return deduped;
}

bool UnionQuery::writesToTheGraph() const {
    return std::ranges::any_of(_branches, [](const Branch& branch) {
        return branch._query->writesToTheGraph();
    });
}

bool UnionQuery::readsTheGraph() const {
    return std::ranges::any_of(_branches, [](const Branch& branch) {
        return branch._query->readsTheGraph();
    });
}
