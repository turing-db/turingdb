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

class CallYieldedIsNullTest : public TuringTest {
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

// A plain string column a procedure yielded holds a value in every row, so IS NOT NULL
// keeps all nine labels and IS NULL keeps none.
TEST_F(CallYieldedIsNullTest, keepsAYieldedStringThatIsNotNull) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label IS NOT NULL RETURN count(label)", sink);

    const std::vector<StringRowSink::Row> expected {{"9"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallYieldedIsNullTest, dropsAYieldedStringForIsNull) {
    StringRowSink sink;
    runQuery("CALL db.labels() YIELD label WHERE label IS NULL RETURN count(label)", sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

TEST_F(CallYieldedIsNullTest, keepsAYieldedUnsignedThatIsNotNull) {
    StringRowSink sink;
    runQuery("CALL db.getNodes([0, 1]) YIELD inEdgeCount WHERE inEdgeCount IS NOT NULL RETURN count(inEdgeCount)", sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}
