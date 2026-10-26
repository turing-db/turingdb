#pragma once

#include <vector>

#include "RegularQuery.h"

namespace db {

class CypherAST;
class Expr;

// WHEN ... THEN ... ELSE: for each row it runs for, the first branch whose predicate is
// true, or the ELSE branch when none is
class ConditionalQuery : public RegularQuery {
public:
    // A WHEN branch holds its predicate; the ELSE branch holds none
    struct Branch {
        Expr* _condition {nullptr};
        RegularQuery* _query {nullptr};
    };

    using Branches = std::vector<Branch>;

    static ConditionalQuery* create(CypherAST* ast, const Branches& branches);

    Kind getKind() const override { return Kind::CONDITIONAL_QUERY; }

    const Branches& branches() const { return _branches; }

    bool writesToTheGraph() const override;
    bool readsTheGraph() const override;

private:
    Branches _branches;

    ConditionalQuery(DeclContext* declContext, const Branches& branches);
    ~ConditionalQuery() override;
};

}
