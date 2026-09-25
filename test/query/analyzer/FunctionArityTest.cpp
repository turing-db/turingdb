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

namespace {

const std::string_view missingArgumentReason = "Invalid arguments for function";

}

// Every function TuringDB declares takes at least one argument, so empty parentheses are
// an arity error the analyzer owns. A call written that way carries no argument list at
// all, where a procedure call written the same way carries an empty one, and reading the
// first as the second is what let avg() reach the code generator.
class FunctionArityTest : public turing::test::TuringTest {
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

    // The query is turned away by the analyzer, on @param reason and as an AnalyzeException:
    // a bioassert tripping downstream would also throw, but as an internal error naming a
    // failed assertion rather than what is wrong with the query
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

    void expectAccepted(const std::string& query) {
        EXPECT_NO_THROW(analyzeQuery(query)) << "query: " << query;
    }

    std::unique_ptr<Graph> _graph;
    std::unique_ptr<ProcedureManager> _procedures;
};

// The reported query: the empty call reached DBProgramGenerator, which asserted on the
// argument it had no way to lower
TEST_F(FunctionArityTest, rejectsAnAggregateWithNoArgumentBesideOneWithAnArgument) {
    expectRejected("MATCH (n) RETURN avg() + count(n)", missingArgumentReason);
}

TEST_F(FunctionArityTest, rejectsAnAggregateWithNoArgument) {
    expectRejected("MATCH (n) RETURN avg()", missingArgumentReason);
}

TEST_F(FunctionArityTest, rejectsAnAggregateWithNoArgumentInAWithClause) {
    expectRejected("MATCH (n) WITH sum() AS total RETURN total", missingArgumentReason);
}

// Nothing about the rule is particular to aggregates: a scalar function called with no
// argument is the same arity error
TEST_F(FunctionArityTest, rejectsAScalarFunctionWithNoArgument) {
    expectRejected("UNWIND range(1, 4) AS p RETURN sum()", missingArgumentReason);
}

TEST_F(FunctionArityTest, keepsAcceptingAnAggregateOverAProperty) {
    expectAccepted("MATCH (n) RETURN avg(n.age) + count(n)");
}

// A procedure declares no argument of its own, so its empty parentheses are the arity it
// has rather than one it is missing
TEST_F(FunctionArityTest, keepsAcceptingAProcedureThatTakesNoArgument) {
    expectAccepted("CALL db.labels() YIELD label RETURN label");
}
