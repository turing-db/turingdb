#include <gtest/gtest.h>

#include "TuringTest.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "iterators/GetEdgeTypesIterator.h"
#include "metadata/EdgeTypeMap.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

namespace {

// The eighteen edges of simpledb: three KNOWS_WELL and fifteen INTERESTED_IN
constexpr size_t edgeCount = 18;
constexpr size_t knowsWellCount = 3;

// Collect every EdgeTypeID the chunk writer produces for the given input, at the given
// per-fill budget, in input order.
void collectEdgeTypes(const GraphReader& reader,
                      const ColumnEdgeIDs* input,
                      size_t maxCount,
                      std::vector<EdgeTypeID>& out) {
    ColumnEdgeTypes edgeTypes;

    GetEdgeTypesChunkWriter writer(reader.getView(), input);
    writer.setEdgeTypes(&edgeTypes);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (const EdgeTypeID id : edgeTypes) {
            out.push_back(id);
        }
    }
}

// Derive the expected EdgeTypeIDs one at a time, through the single-edge accessor
void expectedEdgeTypes(const GraphReader& reader,
                       const ColumnEdgeIDs* input,
                       std::vector<EdgeTypeID>& out) {
    out.clear();
    for (const EdgeID edgeID : *input) {
        out.push_back(reader.getEdgeTypeID(edgeID));
    }
}

void allEdges(ColumnEdgeIDs& input) {
    for (size_t index = 0; index < edgeCount; index++) {
        input.push_back(EdgeID {index});
    }
}

}

class GetEdgeTypesIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(GetEdgeTypesIteratorTest, emptyInput) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnEdgeIDs input;
    GetEdgeTypesIterator it(reader.getView(), &input);
    ASSERT_FALSE(it.isValid());

    std::vector<EdgeTypeID> result;
    collectEdgeTypes(reader, &input, 16, result);
    ASSERT_TRUE(result.empty());
}

TEST_F(GetEdgeTypesIteratorTest, singleEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnEdgeIDs input;
    input.push_back(EdgeID {0});

    const EdgeTypeID expected = reader.getEdgeTypeID(EdgeID {0});

    GetEdgeTypesIterator it(reader.getView(), &input);
    ASSERT_TRUE(it.isValid());
    ASSERT_EQ(it.get(), expected);

    ++it;
    ASSERT_FALSE(it.isValid());
}

TEST_F(GetEdgeTypesIteratorTest, iteratorVisitsAllEdgesInOrder) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnEdgeIDs input;
    allEdges(input);

    std::vector<EdgeTypeID> expected;
    expectedEdgeTypes(reader, &input, expected);

    std::vector<EdgeTypeID> result;
    GetEdgeTypesIterator it(reader.getView(), &input);
    for (; it.isValid(); ++it) {
        result.push_back(it.get());
    }

    ASSERT_EQ(result, expected);
}

// The types read back are the ones the graph was built with, and not merely
// self-consistent: simpledb has exactly three KNOWS_WELL edges among its eighteen.
TEST_F(GetEdgeTypesIteratorTest, readsTheTypeEachEdgeWasCreatedWith) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    const EdgeTypeMap& edgeTypeMap = reader.getMetadata().edgeTypes();
    const std::optional<EdgeTypeID> knowsWell = edgeTypeMap.get("KNOWS_WELL");
    const std::optional<EdgeTypeID> interestedIn = edgeTypeMap.get("INTERESTED_IN");
    ASSERT_TRUE(knowsWell.has_value());
    ASSERT_TRUE(interestedIn.has_value());

    ColumnEdgeIDs input;
    allEdges(input);

    std::vector<EdgeTypeID> result;
    collectEdgeTypes(reader, &input, 16, result);
    ASSERT_EQ(result.size(), edgeCount);

    const size_t knowsWellRows = static_cast<size_t>(
        std::count(result.begin(), result.end(), *knowsWell));
    const size_t interestedInRows = static_cast<size_t>(
        std::count(result.begin(), result.end(), *interestedIn));

    EXPECT_EQ(knowsWellRows, knowsWellCount);
    EXPECT_EQ(interestedInRows, edgeCount - knowsWellCount);
}

TEST_F(GetEdgeTypesIteratorTest, chunkWriterPartialFill) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnEdgeIDs input;
    allEdges(input);

    std::vector<EdgeTypeID> expected;
    expectedEdgeTypes(reader, &input, expected);

    std::vector<EdgeTypeID> result;
    collectEdgeTypes(reader, &input, 1, result);

    ASSERT_EQ(result, expected);
}

TEST_F(GetEdgeTypesIteratorTest, chunkWriterRespectsMaxCount) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnEdgeIDs input;
    for (size_t index = 0; index < 6; index++) {
        input.push_back(EdgeID {index});
    }

    ColumnEdgeTypes edgeTypes;
    GetEdgeTypesChunkWriter writer(reader.getView(), &input);
    writer.setEdgeTypes(&edgeTypes);

    writer.fill(3);
    ASSERT_EQ(edgeTypes.size(), 3u);
    ASSERT_TRUE(writer.isValid());

    writer.fill(3);
    ASSERT_EQ(edgeTypes.size(), 3u);
    ASSERT_FALSE(writer.isValid());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
