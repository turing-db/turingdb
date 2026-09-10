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

class ListIndexAnalysisTest : public turing::test::TuringTest {
public:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
        _procedures = std::make_unique<ProcedureManager>();
        _procedures->init();
    }

protected:
    void analyzeQuery(const std::string& query, bool isV3) {
        CypherAST ast(_procedures.get(), query);

        CypherParser parser(&ast);
        parser.parse(query);

        const FrozenCommitTx transaction = _graph->openTransaction();
        CypherAnalyzer analyzer(&ast, transaction.viewGraph());

        if (isV3) {
            analyzer.setV3();
        }

        analyzer.analyze();
    }

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query, true)) << "query: " << query;
    }

    void expectRejected(const std::string& query, std::string_view reason) {
        try {
            analyzeQuery(query, true);
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

TEST_F(ListIndexAnalysisTest, acceptsALiteralIndexIntoALiteralList) {
    expectAccepted("MATCH (n:Person) RETURN [1, 2, 3][1]");
}

// Cypher counts a negative index from the end of the list, so the rule the CSV row
// access carries - a field position is never negative - does not apply here.
TEST_F(ListIndexAnalysisTest, acceptsANegativeIndex) {
    expectAccepted("MATCH (n:Person) RETURN [1, 2, 3][-1]");
}

TEST_F(ListIndexAnalysisTest, acceptsAnIndexPastTheEndOfTheList) {
    expectAccepted("MATCH (n:Person) RETURN [1, 2, 3][9]");
}

TEST_F(ListIndexAnalysisTest, acceptsAnIndexIntoABoundList) {
    expectAccepted("WITH [1, 2] AS xs RETURN xs[0]");
}

TEST_F(ListIndexAnalysisTest, acceptsAnIndexReadFromARow) {
    expectAccepted("MATCH (n:Person) RETURN [10, 20, 30][n.age]");
}

TEST_F(ListIndexAnalysisTest, acceptsAnIndexIntoAListOfLists) {
    expectAccepted("MATCH (n:Person) RETURN [[1, 2], [3, 4]][0]");
}

TEST_F(ListIndexAnalysisTest, acceptsAComparisonOfAnIndexedElement) {
    expectAccepted("MATCH (n:Person) WHERE [1, 2, 3][0] = 1 RETURN n.name");
    expectAccepted("MATCH (n:Person) WHERE [1, 2, 3][0] = [1, 2, 3][1] RETURN n.name");
    expectAccepted("MATCH (n:Person) WHERE ['a', 'b'][0] = 'a' RETURN n.name");
}

TEST_F(ListIndexAnalysisTest, acceptsANullTestOnAnIndexedElement) {
    expectAccepted("MATCH (n:Person) WHERE [1, 2, 3][9] IS NULL RETURN n.name");
    expectAccepted("MATCH (n:Person) WHERE [1, 2, 3][9] IS NOT NULL RETURN n.name");
}

TEST_F(ListIndexAnalysisTest, rejectsANonIntegerIndex) {
    expectRejected("MATCH (n:Person) RETURN [1, 2, 3]['a']",
                   "Index expression must be an integer");
    expectRejected("MATCH (n:Person) RETURN [1, 2, 3][1.5]",
                   "Index expression must be an integer");
}

TEST_F(ListIndexAnalysisTest, rejectsAnIndexIntoAScalar) {
    expectRejected("MATCH (n:Person) RETURN 1[0]",
                   "Index operator [] can only be applied to a list or a CSV row");
    expectRejected("MATCH (n:Person) RETURN n.name[0]",
                   "Index operator [] can only be applied to a list or a CSV row");
}

TEST_F(ListIndexAnalysisTest, acceptsAChainedIndex) {
    expectAccepted("MATCH (n:Person) RETURN [[1, 2], [3, 4]][0][1]");
}

TEST_F(ListIndexAnalysisTest, acceptsAnIndexIntoAnUnwoundItem) {
    expectAccepted("UNWIND [[1, 2], [3, 4]] AS xs RETURN xs[0]");
    expectAccepted("UNWIND [1, [2, 3]] AS xs RETURN xs[0]");
}

// The rejection for the pipeline engine, which runs no list operator, belongs to its
// planner: the analyzer types the access for both engines.
TEST_F(ListIndexAnalysisTest, typesAListIndexForThePipelineEngineToo) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n:Person) RETURN [1, 2, 3][1]", false));
}
