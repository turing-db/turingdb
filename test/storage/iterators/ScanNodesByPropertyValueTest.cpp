#include <gtest/gtest.h>

#include <stdint.h>
#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "datapart/DataPart.h"
#include "iterators/ScanNodesByPropertyValueIterator.h"
#include "metadata/LabelSet.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeAccessor.h"
#include "versioning/CommitBuilder.h"
#include "versioning/Transaction.h"
#include "views/GraphView.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "TuringTest.h"

using namespace db;
using namespace turing::test;

namespace {

// Long enough that the vectorised blocks, the scalar tail and the chunk bound all get
// exercised. Final node IDs are not creation order, so every expectation is read back
// through the per-node hash lookup rather than derived from these rules.
constexpr size_t firstPartNodeCount = 1000;
constexpr size_t secondPartNodeCount = 333;
constexpr size_t nodeCount = firstPartNodeCount + secondPartNodeCount;

int64_t valueOf(size_t node) {
    return static_cast<int64_t>(node % 7);
}

uint64_t countOf(size_t node) {
    return node % 11;
}

double ratioOf(size_t node) {
    return static_cast<double>(node % 5) / 2.0;
}

bool flagOf(size_t node) {
    return node % 3 == 0;
}

std::string tagOf(size_t node) {
    return "t" + std::to_string(node % 4);
}

using ValueOverride = std::pair<uint64_t, int64_t>;

// Rewritten in the second part; the third part rewrites node 3 to the same value again
// and node 10 once more.
const std::vector<ValueOverride> secondPartOverrides {{3, 99}, {10, 3}, {17, 5}};
const std::vector<ValueOverride> thirdPartOverrides {{3, 99}, {10, 4}};

}

class ScanNodesByPropertyValueTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writePart(0, firstPartNodeCount, {});
        writePart(firstPartNodeCount, secondPartNodeCount, secondPartOverrides);
        writePart(nodeCount, 0, thirdPartOverrides);
    }

    void writePart(size_t firstNode, size_t count, const std::vector<ValueOverride>& valueOverrides) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();
        MetadataBuilder& metadata = builder.getMetadata();

        _rowLabel = metadata.getOrCreateLabel("Row");
        _evenLabel = metadata.getOrCreateLabel("Even");
        _thirdLabel = metadata.getOrCreateLabel("Third");
        _valueID = metadata.getOrCreatePropertyType("value", ValueType::Int64)._id;
        _countID = metadata.getOrCreatePropertyType("count", ValueType::UInt64)._id;
        _ratioID = metadata.getOrCreatePropertyType("ratio", ValueType::Double)._id;
        _flagID = metadata.getOrCreatePropertyType("flag", ValueType::Bool)._id;
        _tagID = metadata.getOrCreatePropertyType("tag", ValueType::String)._id;
        _absentID = metadata.getOrCreatePropertyType("absent", ValueType::Int64)._id;

        for (size_t node = firstNode; node < firstNode + count; node++) {
            LabelSet labelset;
            labelset.set(_rowLabel);
            if (node % 2 == 0) {
                labelset.set(_evenLabel);
            }
            if (node % 3 == 0) {
                labelset.set(_thirdLabel);
            }

            const NodeID nodeID = builder.addNode(labelset);

            builder.addNodeProperty<types::Int64>(nodeID, _valueID, valueOf(node));
            builder.addNodeProperty<types::UInt64>(nodeID, _countID, countOf(node));
            builder.addNodeProperty<types::Double>(nodeID, _ratioID, ratioOf(node));
            builder.addNodeProperty<types::Bool>(nodeID, _flagID, CustomBool(flagOf(node)));

            const std::string tag = tagOf(node);
            builder.addNodeProperty<types::String>(nodeID, _tagID, tag);
        }

        for (const ValueOverride& valueOverride : valueOverrides) {
            builder.addNodeProperty<types::Int64>(NodeID {valueOverride.first}, _valueID, valueOverride.second);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // The value a node currently holds, as the per-node lookup resolves it: the newest
    // part carrying the property for that node wins.
    template <SupportedType T>
    static std::optional<typename T::Primitive> currentValue(const GraphView& view, PropertyTypeID property, uint64_t node) {
        const DataPartSpan parts = view.dataparts();
        for (size_t index = parts.size(); index > 0; index--) {
            const PropertyManager& properties = parts[index - 1]->nodeProperties();
            const typename T::Primitive* value = properties.tryGet<T>(property, EntityID {node});
            if (value) {
                return *value;
            }
        }

        return std::nullopt;
    }

    // The nodes currently holding `value`, restricted to those carrying every label of
    // `labelset` when the handle is valid
    template <SupportedType T>
    static void expectedNodes(const GraphView& view,
                              PropertyTypeID property,
                              const typename T::Primitive& value,
                              const LabelSetHandle& labelset,
                              std::vector<uint64_t>& nodes) {
        const GraphReader reader(view);

        nodes.clear();
        for (uint64_t node = 0; node < nodeCount; node++) {
            const bool labelled = !labelset.isValid() || reader.getNodeLabelSet(NodeID {node}).hasAtLeastLabels(labelset);
            const std::optional<typename T::Primitive> current = currentValue<T>(view, property, node);
            if (labelled && current && *current == value) {
                nodes.push_back(node);
            }
        }
    }

    // Every node the writer yields for `value`, `chunkSize` at a time, sorted by ID
    template <SupportedType T>
    void scan(const GraphView& view,
              PropertyTypeID property,
              const typename T::Primitive& value,
              size_t chunkSize,
              std::vector<uint64_t>& found,
              const LabelSetHandle& labelset = {}) {
        ColumnNodeIDs nodeIDs;
        ScanNodesByPropertyValueChunkWriter<T> writer(view, property, value, labelset);
        writer.setNodeIDs(&nodeIDs);

        found.clear();
        while (writer.isValid()) {
            writer.fill(chunkSize);
            ASSERT_LE(nodeIDs.size(), chunkSize);

            for (const NodeID nodeID : nodeIDs.getRaw()) {
                found.push_back(nodeID.getValue());
            }
        }

        std::sort(found.begin(), found.end());
    }

    template <SupportedType T>
    void expectScan(PropertyTypeID property, const typename T::Primitive& value, const LabelSetHandle& labelset = {}) {
        const FrozenCommitTx transaction = _graph->openTransaction();
        const GraphView view = transaction.viewGraph();

        std::vector<uint64_t> expected;
        expectedNodes<T>(view, property, value, labelset, expected);
        ASSERT_FALSE(expected.empty());

        for (const size_t chunkSize : {size_t {1}, size_t {5}, size_t {8}, size_t {64}, size_t {100000}}) {
            std::vector<uint64_t> found;
            scan<T>(view, property, value, chunkSize, found, labelset);
            EXPECT_EQ(found, expected) << "chunk size " << chunkSize;
        }
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    PropertyTypeID _valueID;
    PropertyTypeID _countID;
    PropertyTypeID _ratioID;
    PropertyTypeID _flagID;
    PropertyTypeID _tagID;
    PropertyTypeID _absentID;
    LabelID _rowLabel;
    LabelID _evenLabel;
    LabelID _thirdLabel;
};

TEST_F(ScanNodesByPropertyValueTest, integerValueAcrossPartsAndChunkSizes) {
    for (const int64_t value : {0, 3, 5, 6}) {
        expectScan<types::Int64>(_valueID, value);
    }
}

TEST_F(ScanNodesByPropertyValueTest, overriddenValuesAreFoundOnlyUnderTheirLatestValue) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    // Node 3 was rewritten to 99 twice: found once, and only under 99.
    std::vector<uint64_t> found;
    scan<types::Int64>(view, _valueID, 99, 64, found);
    const std::vector<uint64_t> onlyNode3 {3};
    EXPECT_EQ(found, onlyNode3);

    // Node 10 was rewritten to 3 and then to 4: found under 4 alone.
    scan<types::Int64>(view, _valueID, 4, 64, found);
    EXPECT_TRUE(std::binary_search(found.begin(), found.end(), 10u));
    scan<types::Int64>(view, _valueID, 3, 64, found);
    EXPECT_FALSE(std::binary_search(found.begin(), found.end(), 10u));

    // Node 17 was rewritten once: found under 5 and under no other value.
    for (const int64_t value : {0, 1, 2, 3, 4, 5, 6}) {
        scan<types::Int64>(view, _valueID, value, 64, found);
        EXPECT_EQ(std::binary_search(found.begin(), found.end(), 17u), value == 5) << "value " << value;
    }
}

TEST_F(ScanNodesByPropertyValueTest, unsignedValue) {
    expectScan<types::UInt64>(_countID, 7);
}

TEST_F(ScanNodesByPropertyValueTest, doubleValue) {
    expectScan<types::Double>(_ratioID, 1.0);
    expectScan<types::Double>(_ratioID, 0.0);
}

TEST_F(ScanNodesByPropertyValueTest, boolValueIsUnselective) {
    expectScan<types::Bool>(_flagID, CustomBool(true));
    expectScan<types::Bool>(_flagID, CustomBool(false));
}

TEST_F(ScanNodesByPropertyValueTest, stringValue) {
    expectScan<types::String>(_tagID, "t2");
}

TEST_F(ScanNodesByPropertyValueTest, labelledIntegerValue) {
    const LabelSet even = LabelSet::fromList({_evenLabel});
    const LabelSet third = LabelSet::fromList({_thirdLabel});
    const LabelSet evenThird = LabelSet::fromList({_evenLabel, _thirdLabel});

    for (const int64_t value : {0, 3, 5}) {
        expectScan<types::Int64>(_valueID, value, LabelSetHandle(even));
        expectScan<types::Int64>(_valueID, value, LabelSetHandle(third));
        expectScan<types::Int64>(_valueID, value, LabelSetHandle(evenThird));
    }
}

TEST_F(ScanNodesByPropertyValueTest, labelledStringAndBoolValues) {
    const LabelSet even = LabelSet::fromList({_evenLabel});

    expectScan<types::String>(_tagID, "t2", LabelSetHandle(even));
    expectScan<types::Bool>(_flagID, CustomBool(false), LabelSetHandle(even));
}

TEST_F(ScanNodesByPropertyValueTest, labelledScanSeesOverridesUnderTheNodeLabels) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();
    const GraphReader reader(view);

    // Node 3 was rewritten to 99 in later parts: found under each label it carries and
    // under no label it lacks.
    const LabelSetHandle node3Labels = reader.getNodeLabelSet(NodeID {3});
    for (const LabelID label : {_rowLabel, _evenLabel, _thirdLabel}) {
        const LabelSet query = LabelSet::fromList({label});
        const LabelSetHandle queryHandle(query);
        const std::vector<uint64_t> expected = node3Labels.hasAtLeastLabels(queryHandle) ? std::vector<uint64_t> {3} : std::vector<uint64_t> {};

        std::vector<uint64_t> found;
        scan<types::Int64>(view, _valueID, 99, 64, found, queryHandle);
        EXPECT_EQ(found, expected) << "label " << label.getValue();
    }
}

TEST_F(ScanNodesByPropertyValueTest, labelNoNodeCarriesYieldsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    LabelSet unknown;
    unknown.set(LabelID {60});

    std::vector<uint64_t> found;
    scan<types::Int64>(view, _valueID, 3, 64, found, LabelSetHandle(unknown));
    EXPECT_TRUE(found.empty());
}

TEST_F(ScanNodesByPropertyValueTest, valueNoNodeHoldsYieldsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<uint64_t> found;
    scan<types::Int64>(view, _valueID, 12345, 64, found);
    EXPECT_TRUE(found.empty());

    scan<types::String>(view, _tagID, "t9", 64, found);
    EXPECT_TRUE(found.empty());
}

TEST_F(ScanNodesByPropertyValueTest, propertyNoPartHoldsYieldsNothing) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    ColumnNodeIDs nodeIDs;
    ScanNodesByPropertyValueChunkWriter<types::Int64> writer(view, _absentID, 1);
    writer.setNodeIDs(&nodeIDs);

    EXPECT_FALSE(writer.isValid());

    writer.fill(64);
    EXPECT_TRUE(nodeIDs.empty());
}
