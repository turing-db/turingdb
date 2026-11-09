#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <array>
#include <span>
#include <string>
#include <string_view>

#include "iterators/ChunkConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "IRTestRows.h"
#include "StringRowSink.h"
#include "WriteQueryTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr std::string_view LABEL_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7", "8"};
constexpr std::string_view EDGE_TYPE_IDS[] = {"0", "1"};
constexpr std::string_view PROPERTY_TYPE_IDS[] = {"0", "1", "2", "3", "4", "5", "6", "7"};
constexpr std::string_view VALUE_TYPES[] = {"Int64", "Int64", "String", "String", "String", "Bool", "Bool", "Bool"};

constexpr std::string_view LABEL_NAMES[] = {
    "Person",
    "SoftwareEngineering",
    "Founder",
    "Bioinformatics",
    "Interest",
    "Exotic",
    "Supernatural",
    "SleepDisturber",
    "Sales",
};

constexpr std::string_view NODE_IDS[] = {
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17",
};

constexpr std::string_view NODE_NAMES[] = {
    "Remy",
    "Adam",
    "Computers",
    "Eighties",
    "Bio",
    "Cooking",
    "Ghosts",
    "Padel",
    "Maxime",
    "Luc",
    "Animals",
    "Martina",
    "Suhas",
    "Gym",
    "Travel",
    "Cyrus",
    "JiuJitsu",
    "Doruk",
};

struct SchemaColumn {
    std::string_view _call;
    std::string_view _yielded;
    std::span<const std::string_view> _values;
};

constexpr std::array<SchemaColumn, 5> SCHEMA_COLUMNS {{
    {"CALL db.labels() YIELD id, label", "id", LABEL_IDS},
    {"CALL db.labels() YIELD id, label", "label", LABEL_NAMES},
    {"CALL db.edgeTypes() YIELD id", "id", EDGE_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD id", "id", PROPERTY_TYPE_IDS},
    {"CALL db.propertyTypes() YIELD valueType", "valueType", VALUE_TYPES},
}};

constexpr std::string_view COUNTED = "MATCH (n) WITH count(n) AS c ";

constexpr std::array<size_t, 3> CHUNK_SIZES {2, 7, ChunkConfig::CHUNK_SIZE};

void valueRows(std::span<const std::string_view> values, Rows& rows) {
    rows.clear();

    for (const std::string_view value : values) {
        rows.push_back({std::string(value)});
    }
}

std::string counted(std::string_view returned) {
    return std::string(COUNTED) + "RETURN " + std::string(returned);
}

}

// A reduce whose accumulator holds values of more than one type keeps it as a column of
// tagged cells. The initial value and what the body computes are written into it as cells,
// whatever column they come from: a plain scalar, a schema ID, a node, a list or a nullable
// value.
class ReduceIntoATaggedCellTest : public WriteQueryTest {
protected:
    void resultRows(std::string_view query, size_t chunkSize, Rows& rows) {
        _interpreter->setChunkSize(chunkSize);

        StringRowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nchunk size: " << chunkSize
                                   << "\nerror: " << status.getError();

        rows = sink.getRows();
    }

    void expectRowsAtEveryChunkSize(std::string_view query, const Rows& expected) {
        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : CHUNK_SIZES) {
            Rows actual;
            resultRows(query, chunkSize, actual);
            std::sort(actual.begin(), actual.end());

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nchunk size: " << chunkSize;
        }
    }
};

TEST_F(ReduceIntoATaggedCellTest, writesAPlainScalarTheBodyComputes) {
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | c)"), {{"18"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | c + 1)"), {{"19"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | c * 0.25)"), {{"4.5"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 0, x IN [1] | toString(c))"), {{"18"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | c > 3)"), {{"true"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | c < 3)"), {{"false"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | EXISTS { MATCH (m) })"), {{"true"}});
    expectRowsAtEveryChunkSize("MATCH (n) RETURN reduce(acc = 'a', x IN [1] | n.name IS NULL), count(*)", {{"false", "18"}});
}

TEST_F(ReduceIntoATaggedCellTest, writesAProcedureYield) {
    for (const SchemaColumn& schema : SCHEMA_COLUMNS) {
        const std::string yielded(schema._yielded);
        const std::string call(schema._call);

        Rows expected;
        valueRows(schema._values, expected);

        expectRowsAtEveryChunkSize(call + " RETURN reduce(acc = 0, x IN [1] | " + yielded + ")", expected);
        expectRowsAtEveryChunkSize(call + " RETURN reduce(acc = [1], x IN [1, 2, 3] | " + yielded + ")", expected);
    }
}

TEST_F(ReduceIntoATaggedCellTest, writesAnEntityAListOrANullableValue) {
    Rows nodes;
    valueRows(NODE_IDS, nodes);

    Rows names;
    valueRows(NODE_NAMES, names);

    expectRowsAtEveryChunkSize("MATCH (n) RETURN reduce(acc = 'a', x IN [1] | n)", nodes);
    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1] | [c])"), {{"[18]"}});
    expectRowsAtEveryChunkSize("MATCH (n) RETURN reduce(acc = 0, x IN [1] | n.name)", names);
    expectRowsAtEveryChunkSize(counted("reduce(acc = 0, x IN [1] | datetime('2020-01-01T00:00:00Z'))"),
                               {{"2020-01-01T00:00:00Z"}});
}

TEST_F(ReduceIntoATaggedCellTest, writesAPlainInitialValue) {
    Rows labelIDs;
    valueRows(LABEL_IDS, labelIDs);

    Rows labelNames;
    valueRows(LABEL_NAMES, labelNames);

    expectRowsAtEveryChunkSize(counted("reduce(acc = c, x IN [1] | 'a')"), {{"a"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = c, x IN [] | 'a')"), {{"18"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = c * 0.25, x IN [] | 'a')"), {{"4.5"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = c > 3, x IN [] | 'a')"), {{"true"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = EXISTS { MATCH (m) }, x IN [] | 'a')"), {{"true"}});
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD id RETURN reduce(acc = id, x IN [] | 'a')", labelIDs);
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD id, label RETURN reduce(acc = id, x IN [1, 2] | label)",
                               labelNames);
}

TEST_F(ReduceIntoATaggedCellTest, changesTheTypeOfTheCellBetweenSteps) {
    Rows labelIDs;
    valueRows(LABEL_IDS, labelIDs);

    Rows labelsPlusOne;
    for (const std::string_view label : LABEL_NAMES) {
        labelsPlusOne.push_back({std::string(label) + "1"});
    }

    expectRowsAtEveryChunkSize(counted("reduce(acc = 'a', x IN [1, 2, 3] | c)"), {{"18"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = c, x IN [1, 2] | toString(acc) + 'x')"), {{"18xx"}});
    expectRowsAtEveryChunkSize(counted("reduce(acc = 0, x IN [1, 2, 3] | CASE WHEN x = 2 THEN toString(c) ELSE acc + 1 END)"),
                               {{"181"}});
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD id, label "
                               "RETURN reduce(acc = 0, x IN [1, 2, 3] | CASE x WHEN 2 THEN label ELSE id END)",
                               labelIDs);
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD label "
                               "RETURN reduce(acc = 0, x IN [1, 2] | CASE WHEN x = 1 THEN label ELSE acc + 1 END)",
                               labelsPlusOne);
}

TEST_F(ReduceIntoATaggedCellTest, decidesAllReduceOverAPlainScalar) {
    Rows everyLabelHolds;
    for (const std::string_view label : LABEL_NAMES) {
        everyLabelHolds.push_back({std::string(label), "true"});
    }

    expectRowsAtEveryChunkSize(counted("allReduce(acc = 'a', x IN [1, 2] | c, acc = 18)"), {{"true"}});
    expectRowsAtEveryChunkSize(counted("allReduce(acc = c, x IN [1] | 'a', acc = 'a')"), {{"true"}});
    expectRowsAtEveryChunkSize(counted("allReduce(acc = 'a', x IN [1, 2] | c + 1, acc < 20)"), {{"true"}});
    expectRowsAtEveryChunkSize(counted("allReduce(acc = 'a', x IN [1, 2] | c * 0.25, acc > 4.5)"), {{"false"}});
    expectRowsAtEveryChunkSize("CALL db.labels() YIELD id, label "
                               "RETURN label, allReduce(acc = 'a', x IN [1] | id, acc = id)",
                               everyLabelHolds);
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
