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

// A list literal in a property position: a list is a stored property value, so the
// analyzer types it as one - the property it creates holds lists, and the mismatches it
// still turns away are the ordinary ones between a property's type and a value of
// another. What the values then store is ListPropertyTest's subject; parsing and analysis
// are enough to reach every verdict here.
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

    void expectAccepted(std::string_view query) {
        try {
            analyzeQuery(query);
        } catch (const TuringException& error) {
            ADD_FAILURE() << "query was rejected: " << query << "\nerror: " << error.what();
        }
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

TEST_F(ListLiteralPropertyValueTest, acceptsAListAsANewNodesProperty) {
    // The property is new here, so nothing but the value's own type decides it: a list
    // value makes a list property.
    expectAccepted("CREATE (x:Thing {vals: [1, 2]})");
}

TEST_F(ListLiteralPropertyValueTest, acceptsAListAssignedToANodeProperty) {
    expectAccepted("MATCH (n) WHERE n.name = 'Remy' SET n.vals = [1, 2]");
}

TEST_F(ListLiteralPropertyValueTest, acceptsAnEmptyListAsANodeProperty) {
    // The type-erased form of the literal, which carries no element to read a type from,
    // still names a list - an empty one, not an absent value.
    expectAccepted("CREATE (x:Thing {vals: []})");
}

TEST_F(ListLiteralPropertyValueTest, acceptsAHeterogeneousListAsANodeProperty) {
    // A stored list is not held to one element type, so the elements need not agree.
    expectAccepted("CREATE (x:Thing {vals: [1, 'two', true]})");
}

TEST_F(ListLiteralPropertyValueTest, rejectsAListMatchedAgainstAStringProperty) {
    // name is a stored string, and a list is not a value it can be compared against.
    expectRejected("MATCH (n {name: [1, 2]}) RETURN n",
                   "Cannot evaluate node property: types 'String' and 'List' are incompatible");
}

TEST_F(ListLiteralPropertyValueTest, rejectsAListAssignedToAStringProperty) {
    expectRejected("MATCH (n) WHERE n.name = 'Remy' SET n.name = [1, 2]",
                   "Cannot evaluate property: types 'String' and 'List' are incompatible");
}
