#include <gtest/gtest.h>

#include <stddef.h>

#include <fstream>
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
#include "iterators/ChunkConfig.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Lists, maps and strings built per row, kept by an op past the step that built them: a
// sort, a union, an optional match, a collect, a group key, a min or max, a
// comprehension or a reduce. Run at chunk sizes of 1 and 3, every value is kept across
// several steps of the op that built it.
class RetainedValuesAcrossChunksTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink, size_t chunkSize) {
        _interpreter->setChunkSize(chunkSize);

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              sink);

        return status;
    }

    void writeDataFile(std::string_view name, std::string_view content) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << content;
    }

    void expectRetainedBy(std::string_view query, std::string_view retainer, const Rows& expected) {
        RowSink explainSink;
        const std::string explained = std::string("EXPLAIN (nl) ") + std::string(query);
        const QueryStatus explainStatus = runQuery(explained, &explainSink, ChunkConfig::CHUNK_SIZE);
        ASSERT_TRUE(explainStatus.isOk()) << explainStatus.getError();
        ASSERT_EQ(explainSink.rows().size(), 1u);

        const std::string& program = explainSink.rows().front().back();
        EXPECT_NE(program.find(retainer), std::string::npos) << query << "\n" << program;

        for (const size_t chunkSize : {size_t {1}, size_t {3}, ChunkConfig::CHUNK_SIZE}) {
            RowSink sink;
            const QueryStatus status = runQuery(query, &sink, chunkSize);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

            Rows rows;
            sink.sortedRows(rows);

            std::string actualText;
            describeRows(rows, actualText);

            EXPECT_EQ(rows, expected) << "query: " << query
                                      << "\nchunk size: " << chunkSize
                                      << "\nactual:\n" << actualText;
        }
    }

private:
    const std::string _graphName {"simpledb"};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(RetainedValuesAcrossChunksTest, sortsValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 5) AS i "
                     "RETURN i, [i, 'v' + toString(i)] AS l, {k: 'w' + toString(i)} AS m, 'x' + toString(i) AS s "
                     "ORDER BY i DESC",
                     "nl.sort_collect",
                     {{"1", "[1, v1]", "{k: w1}", "x1"},
                      {"2", "[2, v2]", "{k: w2}", "x2"},
                      {"3", "[3, v3]", "{k: w3}", "x3"},
                      {"4", "[4, v4]", "{k: w4}", "x4"},
                      {"5", "[5, v5]", "{k: w5}", "x5"}});
}

TEST_F(RetainedValuesAcrossChunksTest, keepsTheTopValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 9) AS i RETURN [toString(i)] AS l, 'x' + toString(i) AS s ORDER BY s DESC LIMIT 2",
                     "nl.sort_collect",
                     {{"[8]", "x8"},
                      {"[9]", "x9"}});
}

TEST_F(RetainedValuesAcrossChunksTest, unitesValuesBuiltPerRow) {
    expectRetainedBy("CALL { UNWIND range(1, 3) AS i RETURN ['a', toString(i)] AS l, 'a' + toString(i) AS s "
                     "UNION ALL "
                     "UNWIND range(1, 3) AS i RETURN ['b', toString(i)] AS l, 'b' + toString(i) AS s } "
                     "RETURN l, s",
                     "nl.union_collect",
                     {{"[a, 1]", "a1"},
                      {"[a, 2]", "a2"},
                      {"[a, 3]", "a3"},
                      {"[b, 1]", "b1"},
                      {"[b, 2]", "b2"},
                      {"[b, 3]", "b3"}});
}

TEST_F(RetainedValuesAcrossChunksTest, collectsValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 5) AS i "
                     "RETURN collect([i, 'v' + toString(i)]) AS l, collect('x' + toString(i)) AS s",
                     "nl.collect_update",
                     {{"[[1, v1], [2, v2], [3, v3], [4, v4], [5, v5]]", "[x1, x2, x3, x4, x5]"}});
}

TEST_F(RetainedValuesAcrossChunksTest, groupsOnKeysBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 7) AS i "
                     "RETURN 'g' + toString(i % 3) AS g, [i % 3] AS l, count(*) AS c",
                     "nl.group_aggregate_update",
                     {{"g0", "[0]", "2"},
                      {"g1", "[1]", "3"},
                      {"g2", "[2]", "2"}});
}

TEST_F(RetainedValuesAcrossChunksTest, keepsTheExtremaOfValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 7) AS i "
                     "RETURN max('s' + toString(i)) AS s, min([8 - i, toString(i)]) AS l, max({k: 'v' + toString(i)}) AS m",
                     "nl.aggregate_update",
                     {{"s7", "[1, 7]", "{k: v7}"}});
}

TEST_F(RetainedValuesAcrossChunksTest, keepsTheGroupedExtremaOfValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 7) AS i "
                     "RETURN i % 2 AS g, max('s' + toString(i)) AS s, min([8 - i, toString(i)]) AS l",
                     "nl.group_aggregate_update",
                     {{"0", "s6", "[2, 6]"},
                      {"1", "s7", "[1, 7]"}});
}

TEST_F(RetainedValuesAcrossChunksTest, padsOptionalRowsCarryingValuesBuiltPerRow) {
    expectRetainedBy("UNWIND range(1, 4) AS i WITH i, ['v' + toString(i)] AS l, 'x' + toString(i) AS s "
                     "OPTIONAL MATCH (n:Person {age: i}) RETURN l, s, n.name",
                     "nl.optional_collect",
                     {{"[v1]", "x1", "null"},
                      {"[v2]", "x2", "null"},
                      {"[v3]", "x3", "null"},
                      {"[v4]", "x4", "null"}});
}

TEST_F(RetainedValuesAcrossChunksTest, buildsAComprehensionOverSeveralChunks) {
    expectRetainedBy("RETURN [x IN range(1, 7) | 'v' + toString(x)] AS l",
                     "nl.list_comprehension",
                     {{"[v1, v2, v3, v4, v5, v6, v7]"}});
}

TEST_F(RetainedValuesAcrossChunksTest, reducesOverSeveralChunks) {
    expectRetainedBy("RETURN reduce(text = '', x IN range(1, 7) | text + toString(x)) AS s, "
                     "reduce(items = [], x IN range(1, 7) | items + ['v' + toString(x)]) AS l",
                     "nl.reduce",
                     {{"1234567", "[v1, v2, v3, v4, v5, v6, v7]"}});
}

TEST_F(RetainedValuesAcrossChunksTest, collectsPerCallRowIntoAnOuterCollect) {
    expectRetainedBy("UNWIND [1, 2, 3] AS k "
                     "CALL (k) { UNWIND range(1, k) AS j RETURN collect(['v' + toString(j)]) AS c } "
                     "RETURN collect(c) AS all",
                     "nl.collect_update",
                     {{"[[[v1]], [[v1], [v2]], [[v1], [v2], [v3]]]"}});
}

TEST_F(RetainedValuesAcrossChunksTest, sortsTheFieldsOfACSVFile) {
    writeDataFile("names.csv", "name\ndelta\nalpha\necho\ncharlie\nbravo\n");

    expectRetainedBy("LOAD CSV 'names.csv' WITH HEADERS AS row RETURN row.name AS name, [row.name] AS l ORDER BY name",
                     "nl.sort_collect",
                     {{"alpha", "[alpha]"},
                      {"bravo", "[bravo]"},
                      {"charlie", "[charlie]"},
                      {"delta", "[delta]"},
                      {"echo", "[echo]"}});
}
