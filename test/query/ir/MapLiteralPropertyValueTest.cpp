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

class MapLiteralPropertyValueTest : public TuringTest {
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

TEST_F(MapLiteralPropertyValueTest, acceptsAMapAsANewNodesProperty) {
    expectAccepted("CREATE (x:Thing {vals: {a: 1, b: 'x'}})");
}

TEST_F(MapLiteralPropertyValueTest, acceptsAMapAssignedToANodeProperty) {
    expectAccepted("MATCH (n) WHERE n.name = 'Remy' SET n.vals = {a: 1}");
}

TEST_F(MapLiteralPropertyValueTest, acceptsAnEmptyMapAsANodeProperty) {
    expectAccepted("CREATE (x:Thing {vals: {}})");
}

TEST_F(MapLiteralPropertyValueTest, acceptsAMapReadingARowAsANodeProperty) {
    expectAccepted("MATCH (n) WHERE n.name = 'Remy' CREATE (x:Thing {vals: {name: n.name}})");
}

TEST_F(MapLiteralPropertyValueTest, acceptsAMapAsANewEdgesProperty) {
    expectAccepted("MATCH (a {name: 'Remy'}), (b {name: 'Adam'}) "
                   "CREATE (a)-[:THING {vals: {a: 1}}]->(b)");
}

TEST_F(MapLiteralPropertyValueTest, rejectsAMapAssignedToAStringProperty) {
    expectRejected("MATCH (n) WHERE n.name = 'Remy' SET n.name = {a: 1}",
                   "Cannot evaluate property: types 'String' and 'Map' are incompatible");
}
