#include "RegularQuery.h"

#include <ranges>

#include "ConditionalQuery.h"
#include "SinglePartQuery.h"
#include "UnionQuery.h"

using namespace db;

namespace {

template <typename Query>
void pushBranches(Query* query, std::vector<Query*>& pending) {
    const QueryCommand::Kind kind = query->getKind();

    if (kind == QueryCommand::Kind::UNION_QUERY) {
        const UnionQuery::Branches& branches = static_cast<const UnionQuery*>(query)->branches();
        for (const UnionQuery::Branch& branch : std::views::reverse(branches)) {
            pending.push_back(branch._query);
        }
    } else if (kind == QueryCommand::Kind::CONDITIONAL_QUERY) {
        const ConditionalQuery::Branches& branches = static_cast<const ConditionalQuery*>(query)->branches();
        for (const ConditionalQuery::Branch& branch : std::views::reverse(branches)) {
            pending.push_back(branch._query);
        }
    }
}

template <typename Query, typename Single>
void collectSingles(Query* query, std::vector<Single*>& queries) {
    std::vector<Query*> pending {query};

    while (!pending.empty()) {
        Query* current = pending.back();
        pending.pop_back();

        if (current->getKind() == QueryCommand::Kind::SINGLE_PART_QUERY) {
            queries.push_back(static_cast<Single*>(current));
        } else {
            pushBranches(current, pending);
        }
    }
}

template <typename Single, typename Query>
Single* firstSingle(Query* query) {
    while (query->getKind() != QueryCommand::Kind::SINGLE_PART_QUERY) {
        if (query->getKind() == QueryCommand::Kind::UNION_QUERY) {
            query = static_cast<const UnionQuery*>(query)->branches().front()._query;
        } else {
            query = static_cast<const ConditionalQuery*>(query)->branches().front()._query;
        }
    }

    return static_cast<Single*>(query);
}

}

RegularQuery::RegularQuery(DeclContext* declContext)
    : QueryCommand(declContext)
{
}

RegularQuery::~RegularQuery() {
}

void RegularQuery::collectSingleQueries(RegularQuery* query, std::vector<SinglePartQuery*>& queries) {
    collectSingles(query, queries);
}

void RegularQuery::collectSingleQueries(const RegularQuery* query, std::vector<const SinglePartQuery*>& queries) {
    collectSingles(query, queries);
}

void RegularQuery::collectQueries(const RegularQuery* query, std::vector<const RegularQuery*>& queries) {
    std::vector<const RegularQuery*> pending {query};

    while (!pending.empty()) {
        const RegularQuery* current = pending.back();
        pending.pop_back();

        queries.push_back(current);
        pushBranches(current, pending);
    }
}

SinglePartQuery* RegularQuery::getFirstSingleQuery() {
    return firstSingle<SinglePartQuery>(this);
}

const SinglePartQuery* RegularQuery::getFirstSingleQuery() const {
    return firstSingle<const SinglePartQuery>(this);
}

bool RegularQuery::isReturning() const {
    return getFirstSingleQuery()->getReturnStmt() != nullptr;
}
