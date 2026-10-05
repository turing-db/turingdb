#pragma once

#include <stddef.h>
#include <vector>

#include "QueryCommand.h"

namespace db {

class CallSubqueryStmt;
class CypherAST;
class SinglePartQuery;

class UnionQuery : public QueryCommand {
public:
    // One branch of the union and the operator joining it to the ones before it. The
    // first branch is joined to nothing, so its flag is never read. A `{ WHEN ... }`
    // branch is written as `CALL () { WHEN ... } RETURN <its columns>`, that CALL held in
    // _whenCall so a CALL holding the union can hand it its imports.
    struct Branch {
        SinglePartQuery* _query {nullptr};
        bool _all {false};
        CallSubqueryStmt* _whenCall {nullptr};
    };

    using Branches = std::vector<Branch>;

    static UnionQuery* create(CypherAST* ast, SinglePartQuery* first, const Branches& rest);

    Kind getKind() const override { return Kind::UNION_QUERY; }

    const Branches& branches() const { return _branches; }

    // How many leading branches dedup against one another, counting from the first.
    // UNION is left associative, so `A UNION ALL B UNION C` is `(A UNION ALL B) UNION C`
    // and its result is distinct(A ++ B ++ C). A dedup earlier in the chain drops only
    // rows a later one would have dropped anyway, so the whole chain collapses to: the
    // branches up to the last distinct operator share one dedup, and the branches after
    // it are appended as they come. Zero for a chain of UNION ALL alone.
    static size_t getDedupedBranchCount(const Branches& branches);

private:
    Branches _branches;

    UnionQuery(DeclContext* declContext);
    ~UnionQuery() override;
};

}
