#pragma once

#include <vector>

#include "QueryCommand.h"

namespace db {

class SinglePartQuery;
class VarDecl;

// A query over the graph: a single query, or a UNION or a WHEN whose every branch is a
// regular query again. The top level holds one, and so does every { } holding a query.
class RegularQuery : public QueryCommand {
public:
    using ImportDecls = std::vector<const VarDecl*>;

    // The single queries under @param query, in the order their branches are written
    static void collectSingleQueries(RegularQuery* query, std::vector<SinglePartQuery*>& queries);
    static void collectSingleQueries(const RegularQuery* query, std::vector<const SinglePartQuery*>& queries);

    // Every query under @param query, itself included, each ahead of its branches
    static void collectQueries(const RegularQuery* query, std::vector<const RegularQuery*>& queries);

    // The single query whose RETURN names the columns this one returns
    SinglePartQuery* getFirstSingleQuery();
    const SinglePartQuery* getFirstSingleQuery() const;

    // Every branch returns the columns of the first or none does
    bool isReturning() const;

    virtual bool writesToTheGraph() const = 0;
    virtual bool readsTheGraph() const = 0;

    // What this query's scope declares for the variables it reads from the scope around
    // it: the imports of a single query of a subquery body, or those the predicates of a
    // WHEN read
    const ImportDecls& getImportDecls() const { return _importDecls; }

    void addImportDecl(const VarDecl* decl) { _importDecls.push_back(decl); }
    void clearImportDecls() { _importDecls.clear(); }

protected:
    explicit RegularQuery(DeclContext* declContext);
    ~RegularQuery() override;

private:
    ImportDecls _importDecls;
};

}
