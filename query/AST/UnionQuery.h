#pragma once

#include <stddef.h>
#include <vector>

#include "QueryCommand.h"

namespace db {

class CypherAST;
class SinglePartQuery;

class UnionQuery : public QueryCommand {
public:
    // One branch of the union and the operator joining it to the ones before it. The
    // first branch is joined to nothing, so its flag is never read.
    struct Branch {
        SinglePartQuery* _query {nullptr};
        bool _all {false};
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
    size_t getDedupedBranchCount() const;

private:
    Branches _branches;

    UnionQuery(DeclContext* declContext);
    ~UnionQuery() override;
};

}
