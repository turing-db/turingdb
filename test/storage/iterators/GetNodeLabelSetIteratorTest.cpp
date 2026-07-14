#include <gtest/gtest.h>

#include "TuringTest.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "columns/ColumnIDs.h"
#include "iterators/GetNodeLabelSetIterator.h"
#include "reader/GraphReader.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"

using namespace db;
using namespace turing::test;

namespace {

// Collect all LabelSetIDs produced by the chunk writer for the given input, using
// the given per-fill budget. Returns results in input order.
void collectLabelSetIDs(const GraphReader& reader,
                        const ColumnNodeIDs* input,
                        size_t maxCount,
                        std::vector<LabelSetID>& out) {
    ColumnLabelSetIDs labelSetIDs;

    GetNodeLabelSetChunkWriter writer(reader.getView(), input);
    writer.setLabelSetIDs(&labelSetIDs);

    out.clear();
    while (writer.isValid()) {
        writer.fill(maxCount);
        for (const LabelSetID id : labelSetIDs) {
            out.push_back(id);
        }
    }
}

// Derive expected LabelSetIDs by calling getNodeLabelSet() for each input node.
void expectedLabelSetIDs(const GraphReader& reader,
                         const ColumnNodeIDs* input,
                         std::vector<LabelSetID>& out) {
    out.clear();
    for (const NodeID nodeID : *input) {
        out.push_back(reader.getNodeLabelSet(nodeID).getID());
    }
}

}

class GetNodeLabelSetIteratorTest : public TuringTest {
protected:
    void initialize() override {
        _graph = Graph::create();
        SimpleGraph::createSimpleGraph(_graph.get());
    }

    std::unique_ptr<Graph> _graph;
};

TEST_F(GetNodeLabelSetIteratorTest, emptyInput) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnNodeIDs input;
    GetNodeLabelSetIterator it(reader.getView(), &input);
    ASSERT_FALSE(it.isValid());

    std::vector<LabelSetID> result;
    collectLabelSetIDs(reader, &input, 16, result);
    ASSERT_TRUE(result.empty());
}

TEST_F(GetNodeLabelSetIteratorTest, singleNode) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Remy is node 0: {Person, SoftwareEngineering, Founder}
    ColumnNodeIDs input;
    input.push_back(NodeID {0});

    const LabelSetID expected = reader.getNodeLabelSet(NodeID {0}).getID();

    GetNodeLabelSetIterator it(reader.getView(), &input);
    ASSERT_TRUE(it.isValid());
    ASSERT_EQ(it.get(), expected);

    ++it;
    ASSERT_FALSE(it.isValid());
}

TEST_F(GetNodeLabelSetIteratorTest, nodesWithSameLabelSet) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Cross datapart nodes which share labelset
    ColumnNodeIDs input;
    input.push_back(NodeID {4});
    input.push_back(NodeID {5});
    input.push_back(NodeID {7});

    std::vector<LabelSetID> result;
    collectLabelSetIDs(reader, &input, 16, result);

    ASSERT_EQ(result.size(), 3u);
    ASSERT_EQ(result[0], result[1]);
    ASSERT_EQ(result[1], result[2]);
}

TEST_F(GetNodeLabelSetIteratorTest, nodesWithDifferentLabelSets) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Remy(0): {Person, SoftwareEngineering, Founder}
    // Adam(1): {Person, Bioinformatics, Founder}
    // Computers(2): {Interest, SoftwareEngineering}
    ColumnNodeIDs input;
    input.push_back(NodeID {0});
    input.push_back(NodeID {1});
    input.push_back(NodeID {2});

    std::vector<LabelSetID> result;
    collectLabelSetIDs(reader, &input, 16, result);

    ASSERT_EQ(result.size(), 3u);
    ASSERT_NE(result[0], result[1]);
    ASSERT_NE(result[1], result[2]);
    ASSERT_NE(result[0], result[2]);
}

TEST_F(GetNodeLabelSetIteratorTest, acrossMultipleDataparts) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // Nodes from different commits/dataparts:
    // 0 (Remy, commit 1), 7 (Padel, commit 2), 9 (Luc, commit 3), 11 (Martina, commit 4)
    ColumnNodeIDs input;
    input.push_back(NodeID {0});
    input.push_back(NodeID {7});
    input.push_back(NodeID {9});
    input.push_back(NodeID {11});

    std::vector<LabelSetID> expected;
    expectedLabelSetIDs(reader, &input, expected);

    std::vector<LabelSetID> result;
    collectLabelSetIDs(reader, &input, 16, result);

    ASSERT_EQ(result, expected);
}

TEST_F(GetNodeLabelSetIteratorTest, iteratorVisitsAllNodesInOrder) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnNodeIDs input;
    input.push_back(NodeID {0});
    input.push_back(NodeID {1});
    input.push_back(NodeID {4});
    input.push_back(NodeID {7});
    input.push_back(NodeID {9});

    std::vector<LabelSetID> expected;
    expectedLabelSetIDs(reader, &input, expected);

    std::vector<LabelSetID> result;
    GetNodeLabelSetIterator it(reader.getView(), &input);
    for (; it.isValid(); ++it) {
        result.push_back(it.get());
    }

    ASSERT_EQ(result, expected);
}

TEST_F(GetNodeLabelSetIteratorTest, chunkWriterPartialFill) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    // 6 nodes, fill one at a time
    ColumnNodeIDs input;
    for (size_t i = 0; i < 6; i++) {
        input.push_back(NodeID {i});
    }

    std::vector<LabelSetID> expected;
    expectedLabelSetIDs(reader, &input, expected);

    std::vector<LabelSetID> result;
    collectLabelSetIDs(reader, &input, 1, result);

    ASSERT_EQ(result, expected);
}

TEST_F(GetNodeLabelSetIteratorTest, chunkWriterRespectsMaxCount) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphReader reader = transaction.readGraph();

    ColumnNodeIDs input;
    for (size_t i = 0; i < 6; i++) {
        input.push_back(NodeID {i});
    }

    ColumnLabelSetIDs labelSetIDs;
    GetNodeLabelSetChunkWriter writer(reader.getView(), &input);
    writer.setLabelSetIDs(&labelSetIDs);

    // First fill should produce exactly 3 results and leave the writer valid
    writer.fill(3);
    ASSERT_EQ(labelSetIDs.size(), 3u);
    ASSERT_TRUE(writer.isValid());

    // Second fill gets the remaining 3
    writer.fill(3);
    ASSERT_EQ(labelSetIDs.size(), 3u);
    ASSERT_FALSE(writer.isValid());
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv, []{});
}
