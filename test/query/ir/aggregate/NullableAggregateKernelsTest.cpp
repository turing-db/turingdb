#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "iterators/ChunkConfig.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// sum, avg, count, min and max over 200 rows of a nullable column, so the validity mask spans
// four words. The columns have no null, a null in every third row, one null that leaves a
// whole word present, or a null on every row.
class NullableAggregateKernelsTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
        addCounters(graph);
    }

    // 200 Counter nodes, i from 1 to 200, with an unsigned property where i is not a multiple of 3.
    // An unsigned integer has no literal, so only a property read gives a nullable UInt64 column.
    void addCounters(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);
        for (uint64_t i = 1; i <= 200; i++) {
            const NodeID counter = writer.addNode({"Counter"});
            if (i % 3 != 0) {
                writer.addNodeProperty<types::UInt64>(counter, "unsigned", uint64_t {i});
            }
        }

        writer.submit();
        jobSystem.terminate();
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

    void expectAggregates(std::string_view column, std::string_view aggregates, const Rows& expected) {
        const std::string query = fmt::format("UNWIND range(1, 200) AS i WITH i, {} AS x RETURN {}", column, aggregates);
        expectRows(query, expected);
    }

    void expectAggregatesInEveryChunking(std::string_view column, std::string_view aggregates, const Rows& expected) {
        for (const size_t chunkSize : {size_t {2}, size_t {7}, ChunkConfig::CHUNK_SIZE}) {
            _interpreter->setChunkSize(chunkSize);
            expectAggregates(column, aggregates, expected);
        }

        _interpreter->setChunkSize(ChunkConfig::CHUNK_SIZE);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

constexpr std::string_view KEYLESS_NUMERIC = "sum(x), min(x), max(x), avg(x), count(x)";
constexpr std::string_view GROUPED_NUMERIC = "i % 4 AS k, sum(x), min(x), max(x), avg(x), count(x)";

TEST_F(NullableAggregateKernelsTest, integersWithNoNull) {
    expectAggregates("CASE WHEN i < 0 THEN null ELSE i END",
                     KEYLESS_NUMERIC,
                     {{"20100", "1", "200", "100.500000", "200"}});
}

TEST_F(NullableAggregateKernelsTest, integersWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE i END",
                     KEYLESS_NUMERIC,
                     {{"13467", "1", "200", "100.500000", "134"}});
}

TEST_F(NullableAggregateKernelsTest, integersAllNull) {
    expectAggregates("CASE WHEN i < 0 THEN i END",
                     KEYLESS_NUMERIC,
                     {{"0", "null", "null", "null", "0"}});
}

TEST_F(NullableAggregateKernelsTest, groupedIntegersWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE i END",
                     GROUPED_NUMERIC,
                     {{"0", "3468", "4", "200", "102.000000", "34"},
                      {"1", "3366", "1", "197", "99.000000", "34"},
                      {"2", "3266", "2", "194", "98.969697", "33"},
                      {"3", "3367", "7", "199", "102.030303", "33"}});
}

TEST_F(NullableAggregateKernelsTest, groupedIntegersWithNullGroups) {
    expectAggregates("CASE WHEN i % 4 < 2 THEN null ELSE i END",
                     GROUPED_NUMERIC,
                     {{"0", "0", "null", "null", "null", "0"},
                      {"1", "0", "null", "null", "null", "0"},
                      {"2", "5000", "2", "198", "100.000000", "50"},
                      {"3", "5050", "3", "199", "101.000000", "50"}});
}

TEST_F(NullableAggregateKernelsTest, doublesWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE toFloat(i) / 4 END",
                     KEYLESS_NUMERIC,
                     {{"3366.750000", "0.250000", "50.000000", "25.125000", "134"}});
}

TEST_F(NullableAggregateKernelsTest, doublesWithANaNAndNoNull) {
    expectAggregates("CASE WHEN i = 50 THEN toFloat('NaN') ELSE toFloat(i) / 4 END",
                     "min(x), max(x)",
                     {{"0.250000", "nan"}});
}

TEST_F(NullableAggregateKernelsTest, doublesWithANaNAndSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null WHEN i = 50 THEN toFloat('NaN') ELSE toFloat(i) / 4 END",
                     KEYLESS_NUMERIC,
                     {{"nan", "0.250000", "nan", "nan", "134"}});
}

TEST_F(NullableAggregateKernelsTest, groupedDoublesWithANaNAndSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null WHEN i = 50 THEN toFloat('NaN') ELSE toFloat(i) / 4 END",
                     "i % 4 AS k, min(x), max(x), count(x)",
                     {{"0", "1.000000", "50.000000", "34"},
                      {"1", "0.250000", "49.250000", "34"},
                      {"2", "0.500000", "nan", "33"},
                      {"3", "1.750000", "49.750000", "33"}});
}

TEST_F(NullableAggregateKernelsTest, doublesAllNull) {
    expectAggregates("CASE WHEN i < 0 THEN toFloat(i) END",
                     KEYLESS_NUMERIC,
                     {{"0.000000", "null", "null", "null", "0"}});
}

TEST_F(NullableAggregateKernelsTest, groupedDoublesWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE toFloat(i) / 4 END",
                     GROUPED_NUMERIC,
                     {{"0", "867.000000", "1.000000", "50.000000", "25.500000", "34"},
                      {"1", "841.500000", "0.250000", "49.250000", "24.750000", "34"},
                      {"2", "816.500000", "0.500000", "48.500000", "24.742424", "33"},
                      {"3", "841.750000", "1.750000", "49.750000", "25.507576", "33"}});
}

TEST_F(NullableAggregateKernelsTest, stringsWithNoNull) {
    expectAggregates("CASE WHEN i < 0 THEN null ELSE toString(i) END",
                     "min(x), max(x)",
                     {{"1", "99"}});
}

TEST_F(NullableAggregateKernelsTest, stringsWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE toString(i) END",
                     "min(x), max(x), count(x)",
                     {{"1", "98", "134"}});
}

TEST_F(NullableAggregateKernelsTest, stringsAllNull) {
    expectAggregates("CASE WHEN i < 0 THEN toString(i) END",
                     "min(x), max(x), count(x)",
                     {{"null", "null", "0"}});
}

TEST_F(NullableAggregateKernelsTest, groupedStringsWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE toString(i) END",
                     "i % 4 AS k, min(x), max(x)",
                     {{"0", "100", "92"},
                      {"1", "1", "97"},
                      {"2", "10", "98"},
                      {"3", "103", "95"}});
}

TEST_F(NullableAggregateKernelsTest, integersWithAWholeWordPresent) {
    expectAggregates("CASE WHEN i = 70 THEN null ELSE i END",
                     KEYLESS_NUMERIC,
                     {{"20030", "1", "200", "100.653266", "199"}});
}

TEST_F(NullableAggregateKernelsTest, groupedIntegersWithAWholeWordPresent) {
    expectAggregates("CASE WHEN i = 70 THEN null ELSE i END",
                     GROUPED_NUMERIC,
                     {{"0", "5100", "4", "200", "102.000000", "50"},
                      {"1", "4950", "1", "197", "99.000000", "50"},
                      {"2", "4930", "2", "198", "100.612245", "49"},
                      {"3", "5050", "3", "199", "101.000000", "50"}});
}

TEST_F(NullableAggregateKernelsTest, stringsWithAWholeWordPresent) {
    expectAggregates("CASE WHEN i = 70 THEN null ELSE toString(i) END",
                     "min(x), max(x), count(x)",
                     {{"1", "99", "199"}});
}

TEST_F(NullableAggregateKernelsTest, pointsWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE point({x: i, y: 2 * i}) END",
                     "avg(x), count(x)",
                     {{"point({x: 100.5, y: 201.0, crs: 'cartesian'})", "134"}});
}

TEST_F(NullableAggregateKernelsTest, pointsAllNull) {
    expectAggregates("CASE WHEN i < 0 THEN point({x: i, y: i}) END",
                     "avg(x), count(x)",
                     {{"null", "0"}});
}

TEST_F(NullableAggregateKernelsTest, durationsWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE duration({seconds: i}) END",
                     "sum(x), avg(x), count(x)",
                     {{"PT3H44M27S", "PT1M40.5S", "134"}});
}

TEST_F(NullableAggregateKernelsTest, groupedDurationsWithSomeNulls) {
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE duration({seconds: i}) END",
                     "i % 4 AS k, sum(x), count(x)",
                     {{"0", "PT57M48S", "34"},
                      {"1", "PT56M6S", "34"},
                      {"2", "PT54M26S", "33"},
                      {"3", "PT56M7S", "33"}});
}

TEST_F(NullableAggregateKernelsTest, durationsAllNull) {
    expectAggregates("CASE WHEN i < 0 THEN duration({seconds: i}) END",
                     "sum(x), avg(x), count(x)",
                     {{"PT0S", "null", "0"}});
}

// A null row adds the +0.0 its slot holds, and -0.0 + 0.0 is +0.0: the sum agrees with the
// one over the same values with no null
TEST_F(NullableAggregateKernelsTest, negativeZeroSumsWithSomeNulls) {
    expectAggregates("CASE WHEN i < 0 THEN null ELSE -0.0 END",
                     "sum(x), count(x)",
                     {{"0.000000", "200"}});
    expectAggregates("CASE WHEN i % 3 = 0 THEN null ELSE -0.0 END",
                     "sum(x), count(x)",
                     {{"0.000000", "134"}});
    expectAggregates("CASE WHEN i % 3 = 0 THEN null WHEN i = 100 THEN -0.0 ELSE toFloat(i) / 4 END",
                     "sum(x), count(x)",
                     {{"3341.750000", "134"}});
}

TEST_F(NullableAggregateKernelsTest, unsignedIntegersWithSomeNulls) {
    expectRows("MATCH (n:Counter) RETURN sum(n.unsigned), min(n.unsigned), max(n.unsigned), avg(n.unsigned), count(n.unsigned)",
               {{"13467", "1", "200", "100.500000", "134"}});
}

// At chunk sizes 2 and 7 a chunk folds into the value an earlier chunk left, and the chunks
// between i = 6 and i = 189 hold no present row
TEST_F(NullableAggregateKernelsTest, valuesKeptAcrossChunks) {
    expectAggregatesInEveryChunking("CASE WHEN i > 5 AND i < 190 THEN null ELSE i END",
                                    KEYLESS_NUMERIC,
                                    {{"2160", "1", "200", "135.000000", "16"}});
    expectAggregatesInEveryChunking("CASE WHEN i > 5 AND i < 190 THEN null ELSE toString(i) END",
                                    "min(x), max(x), count(x)",
                                    {{"1", "5", "16"}});
    expectAggregatesInEveryChunking("CASE WHEN i % 3 = 0 THEN null ELSE toFloat(i) / 4 END",
                                    KEYLESS_NUMERIC,
                                    {{"3366.750000", "0.250000", "50.000000", "25.125000", "134"}});
    expectAggregatesInEveryChunking("CASE WHEN i % 3 = 0 THEN null ELSE duration({seconds: i}) END",
                                    "sum(x), avg(x)",
                                    {{"PT3H44M27S", "PT1M40.5S"}});
    expectAggregatesInEveryChunking("CASE WHEN i % 3 = 0 THEN null ELSE point({x: i, y: 2 * i}) END",
                                    "avg(x)",
                                    {{"point({x: 100.5, y: 201.0, crs: 'cartesian'})"}});
    expectAggregatesInEveryChunking("CASE WHEN i = 70 THEN null ELSE i END",
                                    GROUPED_NUMERIC,
                                    {{"0", "5100", "4", "200", "102.000000", "50"},
                                     {"1", "4950", "1", "197", "99.000000", "50"},
                                     {"2", "4930", "2", "198", "100.612245", "49"},
                                     {"3", "5050", "3", "199", "101.000000", "50"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
