#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

// The dimension a CREATE VECTOR INDEX may ask for runs from 1 to 8192: both ends are
// analyzed, both the values past them are turned away.
class CreateVectorIndexDimensionTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());

        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    void analyzeQuery(const std::string& query) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph());

        analyzer.analyze();
    }

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query)) << "query: " << query;
    }

    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(CreateVectorIndexDimensionTest, acceptsTheSmallestDimension) {
    expectAccepted("CREATE VECTOR INDEX vectors WITH DIMENSION 1 METRIC EUCLID");
}

TEST_F(CreateVectorIndexDimensionTest, acceptsTheLargestDimension) {
    expectAccepted("CREATE VECTOR INDEX vectors WITH DIMENSION 8192 METRIC EUCLID");
}

TEST_F(CreateVectorIndexDimensionTest, rejectsADimensionOfZero) {
    expectRejected("CREATE VECTOR INDEX vectors WITH DIMENSION 0 METRIC EUCLID",
                   "dimension must be greater than 0");
}

TEST_F(CreateVectorIndexDimensionTest, rejectsADimensionPastTheLargest) {
    expectRejected("CREATE VECTOR INDEX vectors WITH DIMENSION 8193 METRIC EUCLID",
                   "dimension must not exceed 8192");
}

TEST_F(CreateVectorIndexDimensionTest, boundsTheDimensionOfAnHNSWIndexToo) {
    expectRejected("CREATE VECTOR INDEX vectors WITH DIMENSION 100000 METRIC EUCLID TYPE HNSW",
                   "dimension must not exceed 8192");
}

TEST_F(CreateVectorIndexDimensionTest, rejectsADimensionPastTheRangeOfTheDimensionType) {
    expectRejected("CREATE VECTOR INDEX vectors WITH DIMENSION 4294975488 METRIC EUCLID",
                   "dimension must not exceed 8192");
    expectRejected("CREATE VECTOR INDEX vectors WITH DIMENSION 4294967296 METRIC EUCLID",
                   "dimension must not exceed 8192");
}
