#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

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

class MapPredicateOperandTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MapPredicateOperandTest, findsNoNullMapBuiltPerRow) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m IS NULL RETURN count(*)", {{"0"}});
}

TEST_F(MapPredicateOperandTest, findsEveryMapBuiltPerRowNotNull) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m IS NOT NULL RETURN count(*)", {{"8"}});
}

TEST_F(MapPredicateOperandTest, findsAMapLiteralNotNull) {
    expectRows("UNWIND [1] AS x WITH x WHERE {a: 1} IS NULL RETURN count(x)", {{"0"}});
}

TEST_F(MapPredicateOperandTest, comparesAMapBuiltPerRowUnequalToAnInteger) {
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m = 32 RETURN count(*)", {{"0"}});
    expectRows("MATCH (n:Person) WITH {a: n.age} AS m WHERE m <> 32 RETURN count(*)", {{"8"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
