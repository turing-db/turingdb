#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "ExprAnalyzer.h"
#include "Graph.h"
#include "Literal.h"
#include "ProcedureManager.h"
#include "ReadStmtAnalyzer.h"
#include "SimpleGraph.h"
#include "decl/DeclContext.h"
#include "expr/LiteralExpr.h"
#include "stmt/VectorSearchStmt.h"
#include "versioning/Transaction.h"

using namespace db;

namespace {

constexpr std::string_view indexName = "vectors";

}

// A VECTOR SEARCH searches for the vector the query wrote, so a statement carrying none -
// or one of no dimension - is turned away by the analyzer. The grammar cannot spell either
// shape, so the statements are built here rather than parsed.
class VectorSearchQueryVectorTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    void expectRejected(Expr* queryVector) {
        CypherAST ast(_procedures.get(), "");

        VectorSearchStmt* statement = VectorSearchStmt::create(&ast);
        statement->setIndex(LiteralExpr::create(&ast, StringLiteral::create(&ast, indexName)));
        statement->setK(LiteralExpr::create(&ast, IntegerLiteral::create(&ast, 3)));
        statement->setQueryVector(queryVector);

        const FrozenCommitTx transaction = _graph->openTransaction();
        DeclContext* declContext = DeclContext::create(&ast, nullptr);

        ExprAnalyzer exprAnalyzer(&ast, transaction.viewGraph());
        exprAnalyzer.setDeclContext(declContext);

        ReadStmtAnalyzer analyzer(&ast, transaction.viewGraph());
        analyzer.setExprAnalyzer(&exprAnalyzer);
        analyzer.setDeclContext(declContext);

        const VectorSearchStmt* const search = statement;
        EXPECT_THROW(analyzer.analyze(search), AnalyzeException);
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(VectorSearchQueryVectorTest, rejectsASearchCarryingNoQueryVector) {
    expectRejected(nullptr);
}

TEST_F(VectorSearchQueryVectorTest, rejectsASearchForAVectorOfNoDimension) {
    CypherAST ast(_procedures.get(), "");
    expectRejected(LiteralExpr::create(&ast, EmbeddingLiteral::create(&ast)));
}
