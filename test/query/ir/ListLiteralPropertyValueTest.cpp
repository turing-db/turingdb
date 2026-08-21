#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "CypherAST.h"
#include "CypherAnalyzer.h"
#include "CypherParser.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

#include "TuringException.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A list exists only in the query language: property values are scalars and the storage
// layer cannot represent a list one. A list literal is the expression that would violate
// that, so every property position has to turn it away - in the analyzer, with a message
// naming the type, rather than downstream where it would read as an engine failure.
// Parsing and analysis are enough to reach all three rejections.
class ListLiteralPropertyValueTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
    }

    void analyzeQuery(std::string_view query) {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        const ProcedureManager* procedures = system.getProcedures();

        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        CypherAST ast(procedures, query);

        CypherParser parser(&ast);
        parser.parse(query);

        CypherAnalyzer analyzer(&ast, view);
        analyzer.setV3();
        analyzer.analyze();
    }

    void expectRejected(std::string_view query, std::string_view reason) {
        try {
            analyzeQuery(query);
        } catch (const TuringException& error) {
            const std::string message = error.what();
            EXPECT_NE(message.find(reason), std::string::npos)
                << "query: " << query << "\nerror: " << message;
            return;
        }

        ADD_FAILURE() << "query was accepted: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
};

TEST_F(ListLiteralPropertyValueTest, rejectsAListAsANewNodesProperty) {
    // No property type holds a list, so there is none to write the value as - the
    // property is new here, so nothing but the value's own type decides it.
    expectRejected("CREATE (x:Thing {vals: [1, 2]})",
                   "Cannot evaluate node property: unsupported type 'List'");
}

TEST_F(ListLiteralPropertyValueTest, rejectsAListAssignedToANodeProperty) {
    expectRejected("MATCH (n) WHERE n.name = 'Remy' SET n.vals = [1, 2]",
                   "Cannot evaluate property: types 'Invalid' and 'List' are incompatible");
}

TEST_F(ListLiteralPropertyValueTest, rejectsAListMatchedAgainstANodeProperty) {
    // The read side of the same invariant: name is a stored string, and a list is not a
    // value it can be compared against.
    expectRejected("MATCH (n {name: [1, 2]}) RETURN n",
                   "Cannot evaluate node property: types 'String' and 'List' are incompatible");
}

TEST_F(ListLiteralPropertyValueTest, rejectsAnEmptyListAsANodeProperty) {
    // The type-erased form of the literal, which carries no element to read a type from,
    // is turned away by the same rule rather than slipping through as an absent value.
    expectRejected("CREATE (x:Thing {vals: []})",
                   "Cannot evaluate node property: unsupported type 'List'");
}
