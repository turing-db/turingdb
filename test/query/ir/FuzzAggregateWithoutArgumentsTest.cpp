#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

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

// AFL inputs that tripped the 'Aggregate function invocation with no arguments' assertion
// of DBProgramGenerator. An aggregate needs an argument, so these are invalid Cypher.
class FuzzAggregateWithoutArgumentsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectAnalyzeError(std::string_view query) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_EQ(status.getStatus(), QueryStatus::Status::ANALYZE_ERROR) << "query: " << query << "\nerror: " << status.getError();
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzAggregateWithoutArgumentsTest, Return000147) {
    expectAnalyzeError("UNWIND range(1, 4) AS x RETURN sum( )");
}

TEST_F(FuzzAggregateWithoutArgumentsTest, ReturnSum) {
    expectAnalyzeError("RETURN sum()");
}

TEST_F(FuzzAggregateWithoutArgumentsTest, ReturnAvg) {
    expectAnalyzeError("RETURN avg()");
}

TEST_F(FuzzAggregateWithoutArgumentsTest, ReturnMin) {
    expectAnalyzeError("RETURN min()");
}

TEST_F(FuzzAggregateWithoutArgumentsTest, ReturnGroupedMax) {
    expectAnalyzeError("MATCH (n) RETURN n.name, max()");
}
