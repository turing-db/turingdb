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

class ScalarSelectionTypingTest : public turing::test::TuringTest {
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

TEST_F(ScalarSelectionTypingTest, rejectsInOverACaseOfNumbers) {
    expectRejected("UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN 1 ELSE 2.0 END",
                   "IN operand must be a list or map, not 'Integer or Double'");
}

TEST_F(ScalarSelectionTypingTest, rejectsInOverACaseOfScalarsOfDifferentKinds) {
    expectRejected("UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN 1 ELSE 'a' END",
                   "IN operand must be a list or map, not 'Integer or String'");
    expectRejected("UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN 1 WHEN NOT b THEN 'a' END",
                   "IN operand must be a list or map, not 'Integer or String'");
    expectRejected("MATCH (n:Person) UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN n ELSE 1 END",
                   "IN operand must be a list or map, not 'NodePattern or Integer'");
}

TEST_F(ScalarSelectionTypingTest, rejectsInOverACoalesceOfScalars) {
    expectRejected("RETURN 1 IN coalesce(null, 1, 'a')",
                   "IN operand must be a list or map, not 'Integer or String'");
}

TEST_F(ScalarSelectionTypingTest, rejectsInOverAVariableBoundToACaseOfScalars) {
    expectRejected("UNWIND [true, false] AS b WITH CASE WHEN b THEN 1 ELSE 2.0 END AS v RETURN 1 IN v",
                   "IN operand must be a list or map, not 'Integer or Double'");
}

TEST_F(ScalarSelectionTypingTest, rejectsInOverAUnionColumnOfScalars) {
    expectRejected("CALL { RETURN 1 AS v UNION ALL RETURN 'a' AS v } RETURN 1 IN v",
                   "IN operand must be a list or map, not 'Integer or String'");
}

TEST_F(ScalarSelectionTypingTest, rejectsAComprehensionOverACaseOfScalars) {
    expectRejected("UNWIND [true, false] AS b RETURN [x IN CASE WHEN b THEN 1 ELSE 2.0 END | x]",
                   "A list comprehension iterates a list, not 'Integer or Double'");
}

TEST_F(ScalarSelectionTypingTest, rejectsAQuantifierOverACaseOfScalars) {
    for (const std::string quantifier : {"all", "any", "none", "single"}) {
        const std::string query = "UNWIND [true, false] AS b RETURN " + quantifier + "(x IN CASE WHEN b THEN 1 ELSE 2.0 END WHERE x > 0)";

        expectRejected(query, "A list comprehension iterates a list, not 'Integer or Double'");
    }
}

TEST_F(ScalarSelectionTypingTest, rejectsAReduceOverACaseOfScalars) {
    expectRejected("UNWIND [true, false] AS b RETURN reduce(s = 0, x IN CASE WHEN b THEN 1 ELSE 2.0 END | s + x)",
                   "A reduce iterates a list, not 'Integer or Double'");
}

TEST_F(ScalarSelectionTypingTest, rejectsASliceOfACaseOfScalars) {
    expectRejected("UNWIND [true, false] AS b RETURN (CASE WHEN b THEN 1 ELSE 2.0 END)[0..1]",
                   "A slice reads a list, not 'Integer or Double'");
}

TEST_F(ScalarSelectionTypingTest, rejectsAnOperatorNoBranchOfACaseTakes) {
    expectRejected("UNWIND [true, false] AS b RETURN CASE WHEN b THEN 'a' ELSE true END - 1",
                   "Operands are not valid and compatible numeric types: 'String or Bool' and 'Integer'");
}

TEST_F(ScalarSelectionTypingTest, acceptsInOverACaseWithACollectionBranch) {
    expectAccepted("UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN [1] ELSE 2.0 END");
    expectAccepted("UNWIND [true, false] AS b RETURN 'a' IN CASE WHEN b THEN {a: 1} ELSE 2 END");
    expectAccepted("UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN [1] ELSE [2.0] END");
    expectAccepted("UNWIND [true, false] AS b WITH CASE WHEN b THEN [1] ELSE 2.0 END AS v RETURN 1 IN v");
}

TEST_F(ScalarSelectionTypingTest, acceptsInOverACell) {
    expectAccepted("UNWIND [[1], 1] AS x RETURN 1 IN x");
    expectAccepted("UNWIND [[1], 1] AS x UNWIND [true, false] AS b RETURN 1 IN CASE WHEN b THEN x ELSE 2 END");
}

TEST_F(ScalarSelectionTypingTest, acceptsAComprehensionOverACaseWithAListBranch) {
    expectAccepted("UNWIND [true, false] AS b RETURN [x IN CASE WHEN b THEN [1] ELSE 2.0 END | x]");
    expectAccepted("UNWIND [true, false] AS b RETURN all(x IN CASE WHEN b THEN [1] ELSE 2.0 END WHERE x > 0)");
    expectAccepted("UNWIND [true, false] AS b RETURN reduce(s = 0, x IN CASE WHEN b THEN [1] ELSE 2.0 END | s + x)");
    expectAccepted("UNWIND [true, false] AS b RETURN (CASE WHEN b THEN [1] ELSE 2.0 END)[0..1]");
}

TEST_F(ScalarSelectionTypingTest, acceptsAnOperatorOneBranchOfACaseTakes) {
    expectAccepted("UNWIND [true, false] AS b RETURN CASE WHEN b THEN 1 ELSE 'a' END - 1");
    expectAccepted("UNWIND [true, false] AS b RETURN CASE WHEN b THEN 1 ELSE 'a' END + 1");
}

TEST_F(ScalarSelectionTypingTest, acceptsAReduceWhoseAccumulatorTakesAnotherType) {
    expectAccepted("UNWIND [true, false] AS b RETURN reduce(acc = CASE WHEN b THEN 'a' ELSE true END, x IN [1] | x) - 1");
    expectAccepted("UNWIND [true, false] AS b RETURN 1 IN reduce(acc = CASE WHEN b THEN 1 ELSE 'a' END, x IN [1] | [x])");
}
