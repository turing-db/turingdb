#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "Graph.h"
#include "Literal.h"
#include "ProcedureManager.h"
#include "ReadStmtAnalyzer.h"
#include "SimpleGraph.h"
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
    void expectRejected(EmbeddingLiteral* queryVector) {
        CypherAST ast(_procedures.get(), "");

        VectorSearchStmt* statement = VectorSearchStmt::create(&ast);
        statement->setIndexName(indexName);
        statement->setK(3);
        statement->setQueryVector(queryVector);

        const FrozenCommitTx transaction = _graph->openTransaction();
        ReadStmtAnalyzer analyzer(&ast, transaction.viewGraph());

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
    expectRejected(EmbeddingLiteral::create(&ast));
}
