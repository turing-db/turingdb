#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "AnalyzeException.h"
#include "TuringTest.h"

#include "CypherAST.h"
#include "CypherParser.h"
#include "CypherAnalyzer.h"
#include "Graph.h"
#include "ProcedureManager.h"
#include "SimpleGraph.h"
#include "versioning/Transaction.h"

using namespace db;

// Every quantifier spelling the engine turns away.
class VariableLengthPathRejectionTest : public turing::test::TuringTest {
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

    void expectRejected(const std::string& query) {
        try {
            analyzeQuery(query);
        } catch (const AnalyzeException& error) {
            const std::string message = error.what();
            const std::string_view reason = "Variable-length paths are not supported yet";

            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

TEST_F(VariableLengthPathRejectionTest, rejectsAStarQuantifier) {
    expectRejected("MATCH (n:Person)-[e]->*(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAPlusQuantifier) {
    expectRejected("MATCH (n:Person)-[e]->+(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsABoundedRange) {
    expectRejected("MATCH (n:Person)-[e]->{2,4}(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnExactHopCount) {
    expectRejected("MATCH (n:Person)-[e]->{3}(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnOpenEndedRange) {
    expectRejected("MATCH (n:Person)-[e]->{2,}(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnOpenStartedRange) {
    expectRejected("MATCH (n:Person)-[e]->{,3}(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnUnboundedRange) {
    expectRejected("MATCH (n:Person)-[e]->{,}(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnUndirectedQuantifier) {
    expectRejected("MATCH (n:Person)-[e]-*(m) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnUndirectedPlusQuantifier) {
    expectRejected("MATCH (n:Person)-[e]-+(m) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAnUndirectedRangeQuantifier) {
    expectRejected("MATCH (n:Person)-[e]-{2,4}(m) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsABackwardQuantifier) {
    expectRejected("MATCH (n:Person)<-[e]-*(m:Person) RETURN n.name, m.name");
}

// The quantifier is one hop of a longer element: the whole pattern goes with it.
TEST_F(VariableLengthPathRejectionTest, rejectsAQuantifierInsideALongerPattern) {
    expectRejected("MATCH (i:Supernatural)-->(n:Person)-[e]->*(m:Person) RETURN i.name, m.name");
}

// A quantified edge carrying a type or a property filter names the quantifier as the
// reason, not the filter.
TEST_F(VariableLengthPathRejectionTest, rejectsATypedQuantifiedEdge) {
    expectRejected("MATCH (n:Person)-[e:KNOWS_WELL]->+(m:Person) RETURN n.name, m.name");
}

TEST_F(VariableLengthPathRejectionTest, rejectsAQuantifiedEdgeWithAPropertyFilter) {
    expectRejected("MATCH (n:Person)-[e {name: 'x'}]->+(m:Person) RETURN n.name, m.name");
}

// A malformed range keeps naming its own defect.
TEST_F(VariableLengthPathRejectionTest, rejectsAnInvertedRangeOnItsOwnReason) {
    try {
        analyzeQuery("MATCH (n:Person)-[e]->{4,2}(m:Person) RETURN n.name");
        ADD_FAILURE() << "query was accepted";
    } catch (const AnalyzeException& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("greater than or equal to minimum hops"), std::string::npos) << message;
    }
}

TEST_F(VariableLengthPathRejectionTest, rejectsAZeroMaximumOnItsOwnReason) {
    try {
        analyzeQuery("MATCH (n:Person)-[e]->{0}(m:Person) RETURN n.name");
        ADD_FAILURE() << "query was accepted";
    } catch (const AnalyzeException& error) {
        const std::string message = error.what();
        EXPECT_NE(message.find("greater than or equal to 1"), std::string::npos) << message;
    }
}

// A pattern with no quantifier is untouched by the rejection.
TEST_F(VariableLengthPathRejectionTest, keepsAcceptingASingleHopPattern) {
    EXPECT_NO_THROW(analyzeQuery("MATCH (n:Person)-[e]->(m:Person) RETURN n.name, m.name"));
    EXPECT_NO_THROW(analyzeQuery("MATCH (n:Person)-[e:KNOWS_WELL]->(m:Person) RETURN n.name, m.name"));
}
