#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

class CallYieldStarTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// CALL db.labels() YIELD * RETURN label: inside a query, YIELD * names every return value
// the procedure declares, so the projection can read any of them.
TEST_F(CallYieldStarTest, yieldStarNamesEveryReturnValue) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD * RETURN label", sink);

    std::vector<StringRowSink::Row> rows;
    sink.sortedRows(rows);

    const std::vector<StringRowSink::Row> expected {{"Bioinformatics"},
                                                    {"Exotic"},
                                                    {"Founder"},
                                                    {"Interest"},
                                                    {"Person"},
                                                    {"Sales"},
                                                    {"SleepDisturber"},
                                                    {"SoftwareEngineering"},
                                                    {"Supernatural"}};
    EXPECT_EQ(rows, expected);
}

TEST_F(CallYieldStarTest, yieldStarFlowsIntoReturnStar) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD * RETURN *", sink);

    const std::vector<std::string> expectedNames {"id", "label"};
    EXPECT_EQ(sink.getNames(), expectedNames);
    EXPECT_EQ(sink.getRows().size(), 9u);
}

// The eight Person nodes crossed with the nine labels.
TEST_F(CallYieldStarTest, yieldStarBesideAMatch) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL db.labels() YIELD * RETURN count(*)", sink);

    const std::vector<StringRowSink::Row> expected {{"72"}};
    EXPECT_EQ(sink.getRows(), expected);
}
