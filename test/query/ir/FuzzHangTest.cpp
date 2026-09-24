#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <chrono>
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

// AFL inputs that ran past the fuzzer's 10 s replay timeout.
class FuzzHangTest : public TuringTest {
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

    void expectRowsWithinDeadline(std::string_view query, size_t rowCount, size_t columnCount) {
        RowSink sink;

        const auto start = std::chrono::steady_clock::now();
        const QueryStatus status = runQuery(query, &sink);
        const auto elapsed = std::chrono::steady_clock::now() - start;

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        const Rows& rows = sink.rows();
        ASSERT_EQ(rows.size(), rowCount) << "query: " << query;

        const bool everyRowHasEveryColumn = std::ranges::all_of(rows, [columnCount](const Row& row) {
            return row.size() == columnCount;
        });
        EXPECT_TRUE(everyRowHasEveryColumn) << "query: " << query;

        const auto elapsedMilliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed);
        EXPECT_LT(elapsed, _deadline) << "query: " << query << "\nelapsed: " << elapsedMilliseconds.count() << " ms";
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);
        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    std::chrono::seconds _deadline {2};
};

TEST_F(FuzzHangTest, Limit000006) {
    expectRowsWithinDeadline("MATCH (a)--> (z),(b)-->(ca)-->(d),(da)-->(b)-->(ca)--> (c),(b)-->(c), (\t\t\t\t\t\t\t\t\t\tbc),(s),(d), (ba)--> (z),(b)-->(ca)-->(dccccccccccccc) RETURN a,b,d,c LIMIT 000", 0, 4);
}

TEST_F(FuzzHangTest, Limit000008) {
    expectRowsWithinDeadline("MATCH (a)--> (z),(b)--> (ba)--> (z),(b)-->(c),(b)-->(c), (bcEMBEDDING),(s),(d),(ca)-->(d),(da)--> (ca)-->(dcccccccccccc)--> (ba)--> (z),(b)-->(c),(bc) RETURN a,b,d,c LIMIT 001", 1, 4);
}

// The pattern is eight disconnected islands, so it matches 36 * 18^4 * 32 * 18 * 18 rows:
// the fuzzer's projection of them runs long because the result is that large.
TEST_F(FuzzHangTest, Match000009) {
    expectRows("MATCH (a)--(bc),(s),(d), (), (bc),(Ia)--> (c),(b)-->(c), (bc),(s),(d), (), (bc),(s),(d), (ba)--> (z),(ccccccccc) RETURN count(*)", {{"39182082048"}});
}

TEST_F(FuzzHangTest, Match000010) {
    expectRowsWithinDeadline("MATCH (a)--(bc),(s),(d), (), (bc),(Ia)--> (c),(b)-->(c), (bc),(s),(d), (), (bc),(s),(d), (ba)--> (z),(ccccccccc) RETURN a,b,d,c LIMIT 1", 1, 4);
}
