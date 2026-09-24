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

// AFL inputs that tripped the 'Misshapen ColumnMasks' assertion of BinaryPredicates.h.
class FuzzMisshapenColumnMasksTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);

        return status;
    }

    void expectNodesZeroAndOne(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        std::string actualText;
        describeRows(actual, actualText);

        const Rows expected {{"0"}, {"1"}};
        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzMisshapenColumnMasksTest, Where000057) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.nama = 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000058) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.ntme = 'Remy' = 03OR n = 1 OR n.namU = 'RGmy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000059) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.nami = 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000060) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.game = 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000061) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.name0= 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000062) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.Pame = 'Remy' = 0 OR n = 1 OR n.name = '[x IN Remy'RETURN n");
}

TEST_F(FuzzMisshapenColumnMasksTest, Where000063) {
    expectNodesZeroAndOne("MATCH (n) WHERE n = 0 OR n = 1 OR n.namV = 'Remy' = 0 OR n = 1 OR n.name = 'Remy'RETURN n");
}
