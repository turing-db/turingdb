#include <gtest/gtest.h>

#include <algorithm>
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

// A map value compares against every kind its tag can hold, as a list element does: the
// kernel reads the tag and answers false where it names another kind
class MapValueComparisonKindsTest : public TuringTest {
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

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"cells"};
};

TEST_F(MapValueComparisonKindsTest, comparesAgainstAnInstant) {
    expectRows("WITH {d: datetime('2024-01-01T00:00:00Z')} AS m "
               "RETURN m.d = datetime('2024-01-01T00:00:00Z'), m.d = datetime('2025-01-01T00:00:00Z')",
               {{"true", "false"}});
}

TEST_F(MapValueComparisonKindsTest, comparesAgainstADuration) {
    expectRows("WITH {d: duration(3600000000)} AS m RETURN m.d = duration(3600000000), m.d = duration(1)",
               {{"true", "false"}});
}

TEST_F(MapValueComparisonKindsTest, comparesAgainstAnEmbedding) {
    expectRows("WITH {e: (1.0, 2.0)} AS m RETURN m.e = (1.0, 2.0), m.e = (1.0, 3.0)", {{"true", "false"}});
}

TEST_F(MapValueComparisonKindsTest, comparesAgainstANodeAndItsId) {
    expectRows("MATCH (n) WHERE n = 0 WITH n, {n: n, i: 0, s: 'a'} AS m RETURN m.n = n, m.i = n, n = m.i, m.s = n",
               {{"true", "true", "true", "false"}});
}

TEST_F(MapValueComparisonKindsTest, comparesAgainstAListElement) {
    expectRows("UNWIND [1, 'a'] AS x WITH x, {a: 1} AS m RETURN x, m.a = x, x = m.a",
               {{"1", "true", "true"}, {"a", "false", "false"}});
}

TEST_F(MapValueComparisonKindsTest, aListElementComparesAgainstAnEmbedding) {
    expectRows("UNWIND [(1.0, 2.0), 'a'] AS x RETURN x = (1.0, 2.0)", {{"true"}, {"false"}});
}
