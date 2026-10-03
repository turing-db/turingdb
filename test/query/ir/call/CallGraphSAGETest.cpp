#include <gtest/gtest.h>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include "iterators/ChunkConfig.h"

#include "CallV3Test.h"
#include "StringRowSink.h"

using namespace db;
using namespace turing::test;

namespace {

// Every row of a single-column result, as text
void column(const StringRowSink& sink, std::vector<std::string>& values) {
    values.clear();

    for (const StringRowSink::Row& row : sink.getRows()) {
        // A query that failed mid-flight can leave a row the sink never finished
        if (row.empty()) {
            continue;
        }

        values.push_back(row.front());
    }
}

// The distinct non-null values of a single-column result, sorted
void distinctColumn(const StringRowSink& sink, std::vector<std::string>& values) {
    std::set<std::string> seen;

    for (const StringRowSink::Row& row : sink.getRows()) {
        if (row.empty() || row.front() == "null") {
            continue;
        }

        seen.insert(row.front());
    }

    values.assign(seen.cbegin(), seen.cend());
}

}

// CALL gnn.graphSAGE([seeds], [fanouts], seed) driven end to end through
// QueryInterpreterV3 on the simpledb fixture. Every argument is constant, so the call
// stands on its own rather than being driven by a MATCH.
class CallGraphSAGETest : public CallV3Test {};

// The call declares three columns per hop, so YIELD * names nine and RETURN * projects
// them in that order.
TEST_F(CallGraphSAGETest, yieldStarNamesNineColumns) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD * RETURN *", sink);

    const std::vector<std::string> expected {"dst_nodes0",
                                             "src_nodes0",
                                             "tgt_nodes0",
                                             "dst_nodes1",
                                             "src_nodes1",
                                             "tgt_nodes1",
                                             "dst_nodes2",
                                             "src_nodes2",
                                             "tgt_nodes2"};
    EXPECT_EQ(sink.getNames(), expected);
}

// A standalone call with no YIELD returns every column the procedure declares.
TEST_F(CallGraphSAGETest, standaloneCallReturnsEveryColumn) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42)", sink);

    EXPECT_EQ(sink.getNames().size(), 9U);
    EXPECT_FALSE(sink.getRows().empty());
}

// One column can be yielded on its own, and the projection reads only that one.
TEST_F(CallGraphSAGETest, yieldsASingleColumn) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD dst_nodes0 RETURN dst_nodes0", sink);

    const std::vector<std::string> expectedNames {"dst_nodes0"};
    EXPECT_EQ(sink.getNames(), expectedNames);

    std::vector<std::string> values;
    distinctColumn(sink, values);

    const std::vector<std::string> expected {"0"};
    EXPECT_EQ(values, expected);
}

// The first hop generates embeddings for exactly the seeds the call names, deduplicated.
TEST_F(CallGraphSAGETest, firstHopDstNodesAreTheSeeds) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([9, 0, 8], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0",
             sink);

    std::vector<std::string> values;
    distinctColumn(sink, values);

    const std::vector<std::string> expected {"0", "8", "9"};
    EXPECT_EQ(values, expected);
}

// A seed named twice is one frontier node, so it is not weighted by how often it appears.
TEST_F(CallGraphSAGETest, repeatedSeedsAreDeduplicated) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 0, 0, 8], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN count(dst_nodes0)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The seed is optional, so the call runs without it.
TEST_F(CallGraphSAGETest, theSeedArgumentMayBeOmitted) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 8], [2, 2, 2]) YIELD dst_nodes0 "
             "RETURN count(dst_nodes0)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"2"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Two calls with the same seed answer the same rows, however many chunks they take.
TEST_F(CallGraphSAGETest, theSameSeedAnswersTheSameRows) {
    StringRowSink first;
    runQuery("CALL gnn.graphSAGE([0, 1, 8, 9], [2, 2, 2], 4242) YIELD tgt_nodes1 "
             "RETURN tgt_nodes1 ORDER BY tgt_nodes1",
             first);

    StringRowSink second;
    runQuery("CALL gnn.graphSAGE([0, 1, 8, 9], [2, 2, 2], 4242) YIELD tgt_nodes1 "
             "RETURN tgt_nodes1 ORDER BY tgt_nodes1",
             second);

    std::vector<std::string> firstRows;
    std::vector<std::string> secondRows;
    column(first, firstRows);
    column(second, secondRows);

    EXPECT_EQ(firstRows, secondRows);
}

// Padding is how nine columns of different lengths are handed over as one result, so a
// row may carry a null in some columns and a node in others.
TEST_F(CallGraphSAGETest, shortColumnsArePaddedWithNull) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [1, 1, 1], 42) YIELD dst_nodes0, src_nodes0 "
             "RETURN dst_nodes0, src_nodes0",
             sink);

    ASSERT_FALSE(sink.getRows().empty());

    size_t seedRows = 0;
    for (const StringRowSink::Row& row : sink.getRows()) {
        ASSERT_FALSE(row.empty());

        if (row.front() != "null") {
            seedRows++;
        }
    }

    EXPECT_EQ(seedRows, 1U) << "one seed should occupy one dst_nodes row";
}

// IS NULL reads the padding, so a projection can keep only the rows a column really
// filled.
TEST_F(CallGraphSAGETest, paddingIsVisibleToIsNull) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 1, 8, 9, 11], [2, 2, 2], 42) YIELD src_nodes0 "
             "WHERE src_nodes0 IS NOT NULL "
             "RETURN count(src_nodes0)",
             sink);

    ASSERT_EQ(sink.getRows().size(), 1U);
    EXPECT_NE(sink.getRows().front().front(), "0");
}

// A yielded node is a node, so its properties read like any other node's.
// A property read off a nullable node column fails to lower: "DBLowering produced an
// invalid nl function". Enable once db.get_node_property accepts a nullable node chunk.
TEST_F(CallGraphSAGETest, DISABLED_aYieldedNodeCarriesItsProperties) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD dst_nodes0 "
             "WHERE dst_nodes0 IS NOT NULL "
             "RETURN dst_nodes0.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A yielded column can be matched on, which is what makes the sample usable as a seed
// set for the rest of a query.
TEST_F(CallGraphSAGETest, aYieldedNodeFlowsIntoAMatch) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD dst_nodes0 "
             "WHERE dst_nodes0 IS NOT NULL "
             "MATCH (n) WHERE n = dst_nodes0 "
             "RETURN n.name",
             sink);

    const std::vector<StringRowSink::Row> expected {{"Remy"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// Two columns of a hop are row aligned, so a projection reading both sees one edge per
// row rather than an unrelated pairing.
// Blocked on the same nullable-node property read as dst_nodes0.name.
TEST_F(CallGraphSAGETest, DISABLED_aHopsSourceAndTargetAreRowAligned) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [4, 1, 1], 42) YIELD src_nodes0, tgt_nodes0 "
             "WHERE src_nodes0 IS NOT NULL "
             "RETURN src_nodes0.name, tgt_nodes0.name",
             sink);

    ASSERT_FALSE(sink.getRows().empty());

    for (const StringRowSink::Row& row : sink.getRows()) {
        ASSERT_EQ(row.size(), 2U);
        EXPECT_EQ(row[0], "Remy") << "hop 0 sampled a node that was not the seed";
        EXPECT_NE(row[1], "null") << "a source with no target";
    }
}

// SKIP and LIMIT apply to the rows the call hands over, as they do to any other source.
TEST_F(CallGraphSAGETest, skipAndLimitApplyToTheYieldedRows) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 1, 8, 9, 11], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN dst_nodes0 SKIP 1 LIMIT 2",
             sink);

    EXPECT_EQ(sink.getRows().size(), 2U);
}

// DISTINCT collapses the padding along with the repeats.
TEST_F(CallGraphSAGETest, distinctCollapsesThePadding) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 8], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN DISTINCT dst_nodes0 ORDER BY dst_nodes0",
             sink);

    std::vector<std::string> values;
    column(sink, values);

    // The two seeds and the one null the longer columns padded the result out with
    ASSERT_FALSE(values.empty());
    EXPECT_LE(values.size(), 3U);

    const std::set<std::string> unique(values.cbegin(), values.cend());
    EXPECT_EQ(unique.size(), values.size()) << "DISTINCT left a duplicate";
}

// collect() gathers a yielded column into one list, which is the shape a caller feeding
// the sample onward wants.
TEST_F(CallGraphSAGETest, aYieldedColumnCollectsIntoAList) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD dst_nodes0 "
             "WHERE dst_nodes0 IS NOT NULL "
             "RETURN collect(dst_nodes0)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A fanout of zero samples nothing for that hop, which is a call that answers with the
// seeds rather than one the engine turns away.
TEST_F(CallGraphSAGETest, aZeroFanoutYieldsOnlyTheSeeds) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([0, 8], [0, 2, 2], 42) YIELD dst_nodes0, src_nodes0 "
             "RETURN count(dst_nodes0), count(src_nodes0)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"2", "0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// A seed the graph does not hold reaches nothing, and the call still answers.
TEST_F(CallGraphSAGETest, anUnknownSeedAnswersWithNoEdges) {
    StringRowSink sink;
    runQuery("CALL gnn.graphSAGE([9999], [2, 2, 2], 42) YIELD src_nodes0 "
             "RETURN count(src_nodes0)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"0"}};
    EXPECT_EQ(sink.getRows(), expected);
}

// The fanout list names one sample size per hop, so any other length names something the
// call cannot act on.
TEST_F(CallGraphSAGETest, rejectsAFanoutListOfTheWrongSize) {
    runQueryExpectingError("CALL gnn.graphSAGE([0], [2, 2]) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Fanout");
    runQueryExpectingError("CALL gnn.graphSAGE([0], [2, 2, 2, 2]) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Fanout");
    runQueryExpectingError("CALL gnn.graphSAGE([0], []) YIELD dst_nodes0 RETURN dst_nodes0",
                           "Fanout");
}

// One node's sample is emitted whole, so a fanout wider than a chunk could not be
// returned without a step running over the row budget it promises.
TEST_F(CallGraphSAGETest, rejectsAFanoutWiderThanAChunk) {
    const std::string overWide = std::to_string(ChunkConfig::CHUNK_SIZE + 1);

    runQueryExpectingError("CALL gnn.graphSAGE([0], [" + overWide + ", 2, 2]) "
                           "YIELD dst_nodes0 RETURN dst_nodes0",
                           "Fanout");
    runQueryExpectingError("CALL gnn.graphSAGE([0], [2, 2, " + overWide + "]) "
                           "YIELD dst_nodes0 RETURN dst_nodes0",
                           "Fanout");
}

// A list of anything but integers names no node, so it is turned away rather than read
// as one.
TEST_F(CallGraphSAGETest, rejectsANonIntegerSeedList) {
    runQueryExpectingError("CALL gnn.graphSAGE(['Remy'], [2, 2, 2]) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "ints");
    runQueryExpectingError("CALL gnn.graphSAGE([0, 'Remy'], [2, 2, 2]) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "ints");
}

// Both list arguments are constant, so a variable read per row cannot supply one.
TEST_F(CallGraphSAGETest, rejectsSeedsReadFromARow) {
    runQueryExpectingError("MATCH (n:Person) "
                           "CALL gnn.graphSAGE([n], [2, 2, 2]) YIELD dst_nodes0 "
                           "RETURN dst_nodes0",
                           "constant");
}

// A name the procedure does not declare cannot be yielded.
TEST_F(CallGraphSAGETest, rejectsAnUndeclaredYield) {
    runQueryExpectingError("CALL gnn.graphSAGE([0], [2, 2, 2]) YIELD dst_nodes3 "
                           "RETURN dst_nodes3",
                           "dst_nodes3");
}

// Every argument is constant, so the call is driven once and its rows are crossed with
// the relation before it: the 8 Person nodes against the 2 rows of a one-seed sample.
TEST_F(CallGraphSAGETest, aMatchBeforeTheCallIsCrossedWithIt) {
    StringRowSink sink;
    runQuery("MATCH (n:Person) CALL gnn.graphSAGE([0], [2, 2, 2], 42) YIELD dst_nodes0 "
             "RETURN count(*)",
             sink);

    const std::vector<StringRowSink::Row> expected {{"16"}};
    EXPECT_EQ(sink.getRows(), expected);
}
