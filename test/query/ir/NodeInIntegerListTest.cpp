#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A node compared to an integer compares its ID, as WHERE n = 0 does
class NodeInIntegerListTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(NodeInIntegerListTest, WhereNodeInList) {
    expectRows("MATCH (n) WHERE n IN [0, 2] RETURN n.name", {{"Computers"}, {"Remy"}});
}

TEST_F(NodeInIntegerListTest, WhereNotNodeInList) {
    expectRows("MATCH (n) WHERE NOT n IN [0] RETURN count(n)", {{"17"}});
}

TEST_F(NodeInIntegerListTest, WhereNodeInListWithNull) {
    expectRows("MATCH (n) WHERE n IN [0, null] RETURN n.name", {{"Remy"}});
}

TEST_F(NodeInIntegerListTest, ReturnNodeInList) {
    expectRows("MATCH (n:Person {name: 'Adam'}) RETURN n IN [1], n IN [0], n IN [0, null]", {{"true", "false", "null"}});
}

TEST_F(NodeInIntegerListTest, WhereEdgeInList) {
    expectRows("MATCH ()-[e]->() WHERE e IN [0, 1] RETURN count(e)", {{"2"}});
}
