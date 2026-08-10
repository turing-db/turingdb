#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherParser.h"
#include "CypherAnalyzer.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "Projection.h"
#include "SimpleGraph.h"
#include "SinglePartQuery.h"
#include "expr/Expr.h"
#include "stmt/ReturnStmt.h"
#include "versioning/Transaction.h"

using namespace db;

// `AS` renames the column an item produces and declares that name as a variable, whatever
// the item is. A bare variable is an item like any other, so `RETURN n AS person` must name
// its column `person` and must let a later clause - an ORDER BY key - name it back.
class ProjectionVariableAliasTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    // Parse and analyze @param query, keeping the AST alive so the projection it built can
    // be inspected afterwards
    void analyzeQuery(std::string_view query) {
        _query = query;
        _ast = std::make_unique<CypherAST>(_procedures.get(), _query);

        CypherParser parser(_ast.get());
        parser.parse(_query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(_ast.get(), transaction.viewGraph());
        analyzer.setV3();

        analyzer.analyze();
    }

    // The names the projection gives its columns, in projection order: the headers a client
    // receives for the result
    void collectColumnNames(std::vector<std::string_view>& names) const {
        const QueryCommand* command = _ast->queries().front();
        ASSERT_EQ(command->getKind(), QueryCommand::Kind::SINGLE_PART_QUERY);

        const SinglePartQuery* query = static_cast<const SinglePartQuery*>(command);
        const ReturnStmt* returnStmt = query->getReturnStmt();
        ASSERT_NE(returnStmt, nullptr);

        const Projection* projection = returnStmt->getProjection();

        for (const Projection::ReturnItem& item : projection->items()) {
            const Expr* const* itemExpr = std::get_if<Expr*>(&item);
            const std::optional<std::string_view> name = itemExpr
                                                             ? projection->getName(*itemExpr)
                                                             : projection->getName(std::get<VarDecl*>(item));

            names.push_back(name.value_or(std::string_view {}));
        }
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
    std::string _query;
    std::unique_ptr<CypherAST> _ast;
};

// The alias is the column's name, so the projection carries `person` and not the name of
// the variable the item happens to read
TEST_F(ProjectionVariableAliasTest, aliasNamesTheColumnOfABareVariable) {
    ASSERT_NO_THROW(analyzeQuery("MATCH (n) RETURN n AS person"));

    std::vector<std::string_view> names;
    collectColumnNames(names);

    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names[0], "person");
}

// The same query with the item a property of the variable rather than the variable itself.
// The two alias forms must not diverge, so this one pins down the expected behaviour.
TEST_F(ProjectionVariableAliasTest, aliasNamesTheColumnOfAProperty) {
    ASSERT_NO_THROW(analyzeQuery("MATCH (n) RETURN n.name AS person"));

    std::vector<std::string_view> names;
    collectColumnNames(names);

    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names[0], "person");
}

// An alias is a declared variable, so a later clause may name it. Nothing about the item
// being a bare variable stops the ORDER BY key from resolving.
TEST_F(ProjectionVariableAliasTest, orderByNamesTheAliasOfABareVariable) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN n AS person ORDER BY person"));
}

TEST_F(ProjectionVariableAliasTest, orderByNamesTheAliasOfAProperty) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN n.name AS person ORDER BY person"));
}

// The key names the alias of a returned column, so the column survives the dedup and the
// DISTINCT does not change whether the key resolves
TEST_F(ProjectionVariableAliasTest, distinctOrderByNamesTheAliasOfABareVariable) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN DISTINCT n AS person ORDER BY person"));
}

TEST_F(ProjectionVariableAliasTest, distinctOrderByNamesTheAliasOfAProperty) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN DISTINCT n.name AS person ORDER BY person"));
}

// Naming the aliased-away variable is not naming the column: `n` is still the variable the
// MATCH declared, and the item reads it, so the key resolves to that same column
TEST_F(ProjectionVariableAliasTest, orderByNamesTheAliasedVariableItself) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n) RETURN DISTINCT n AS person ORDER BY n"));
}

// Without an alias the column takes the variable's own name, which is what the alias
// replaces
TEST_F(ProjectionVariableAliasTest, unaliasedVariableKeepsItsOwnName) {
    ASSERT_NO_THROW(analyzeQuery("MATCH (n) RETURN n"));

    std::vector<std::string_view> names;
    collectColumnNames(names);

    ASSERT_EQ(names.size(), 1u);
    EXPECT_EQ(names[0], "n");
}

// The alias occupies the name it declares: a second item may not take it, whichever of the
// two is the bare variable
TEST_F(ProjectionVariableAliasTest, rejectsAliasCollidingWithAnotherColumn) {
    EXPECT_THROW(analyzeQuery("MATCH (n) RETURN n AS person, n.name AS person"),
                 AnalyzeException);
}
