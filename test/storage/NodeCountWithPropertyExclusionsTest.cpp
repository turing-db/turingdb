#include <gtest/gtest.h>

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <optional>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Tombstones.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/GraphWriter.h"
#include "writers/MetadataBuilder.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

constexpr size_t FIRST_PART_NODE_COUNT = 40;
constexpr size_t RARE_NODE_COUNT = 4;

}

// A node's value for a property is its entry in the newest part holding one, and a deleted
// node has none. The first part holds 40 Row nodes, the first 4 of them Rare as well. A later
// part rewrites nodes 0 and 5 and nulls nodes 1 and 7, a later one deletes nodes 0 and 9, and
// the last adds 2 Row nodes. Node 0 is both rewritten and deleted, so it must be left out once.
// The 4 Rare entries of the first part are fewer than its 6 exclusions and the 40 Row entries
// more, so the two label sets take both ways of finding the excluded entries.
class NodeCountWithPropertyExclusionsTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writePart(FIRST_PART_NODE_COUNT, {}, {});
        writePart(0, {{0, 111}, {5, 555}}, {1, 7});

        GraphWriter writer(_graph.get(), _jobSystem.get());
        writer.deleteNodes({NodeID {0}, NodeID {9}});
        writer.submit();

        writePart(2, {}, {});
    }

    void terminate() override {
        _jobSystem->terminate();
    }

    void writePart(size_t newNodeCount,
                   const std::vector<std::pair<uint64_t, int64_t>>& rewrites,
                   const std::vector<uint64_t>& nulls) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();
        MetadataBuilder& metadata = builder.getMetadata();

        _rowLabel = metadata.getOrCreateLabel("Row");
        _rareLabel = metadata.getOrCreateLabel("Rare");
        _valueID = metadata.getOrCreatePropertyType("value", ValueType::Int64)._id;

        for (size_t node = 0; node < newNodeCount; node++) {
            LabelSet labelset;
            labelset.set(_rowLabel);
            if (_nodeCount < RARE_NODE_COUNT) {
                labelset.set(_rareLabel);
            }

            const NodeID nodeID = builder.addNode(labelset);
            builder.addNodeProperty<types::Int64>(nodeID, _valueID, static_cast<int64_t>(_nodeCount) * 10);
            _nodeCount++;
        }

        for (const auto& [node, value] : rewrites) {
            builder.addNodeProperty<types::Int64>(NodeID {node}, _valueID, value);
        }

        for (const uint64_t node : nulls) {
            builder.addNodeProperty<types::Int64>(NodeID {node}, _valueID, std::nullopt);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // The live nodes of the label set the per-node lookup resolves a value for.
    size_t expectedCount(const GraphReader& reader, const LabelSetHandle& labelset) const {
        const Tombstones& tombstones = reader.getView().tombstones();

        size_t count = 0;
        for (uint64_t node = 0; node < _nodeCount; node++) {
            const NodeID nodeID {node};
            if (tombstones.containsNode(nodeID) || !reader.getNodeLabelSet(nodeID).hasAtLeastLabels(labelset)) {
                continue;
            }

            const std::optional<const int64_t*> value = reader.tryGetNodeProperty<types::Int64>(_valueID, nodeID);
            if (value.has_value() && value.value()) {
                count++;
            }
        }

        return count;
    }

    void expectCount(LabelID label, size_t count) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphReader reader = transaction.readGraph();

        const LabelSet labelset = LabelSet::fromList({label});
        const LabelSetHandle handle(labelset);

        EXPECT_EQ(expectedCount(reader, handle), count);
        EXPECT_EQ(reader.getNodeCountWithProperty(handle, _valueID), count);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    size_t _nodeCount {0};
    PropertyTypeID _valueID;
    LabelID _rowLabel;
    LabelID _rareLabel;
};

// 42 Row nodes, less the 2 deleted and the 2 nulled.
TEST_F(NodeCountWithPropertyExclusionsTest, leavesOutExclusionsFewerThanTheMatchedEntries) {
    expectCount(_rowLabel, 38);
}

// Of the 4 Rare nodes, node 0 is deleted and node 1 nulled.
TEST_F(NodeCountWithPropertyExclusionsTest, leavesOutExclusionsMoreThanTheMatchedEntries) {
    expectCount(_rareLabel, 2);
}
