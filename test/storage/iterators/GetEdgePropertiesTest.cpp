#include <gtest/gtest.h>

#include <stdint.h>
#include <algorithm>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "Graph.h"
#include "JobSystem.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "iterators/GetPropertiesIterator.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
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

constexpr size_t edgeCount = 6;

using EdgeWeight = std::pair<uint64_t, int64_t>;

int64_t weightOf(size_t edge) {
    return static_cast<int64_t>(edge) * 10 + 1;
}

constexpr uint64_t rewrittenEdge = 1;
constexpr uint64_t nulledEdge = 3;

constexpr int64_t rewrittenWeight = 777;

}

class GetEdgePropertiesTest : public TuringTest {
protected:
    void initialize() override {
        _jobSystem = std::make_unique<JobSystem>();
        _jobSystem->init();
        _graph = Graph::create();

        writeChain();
        writeWeightUpdates({{rewrittenEdge, rewrittenWeight}}, {nulledEdge});

        for (uint64_t edge = 0; edge < edgeCount; edge++) {
            _inputEdges.push_back(EdgeID {edge});
        }
    }

    void declareMetadata(MetadataBuilder& metadata) {
        _stopLabel = metadata.getOrCreateLabel("Stop");
        _roadType = metadata.getOrCreateEdgeType("ROAD");
        _weightID = metadata.getOrCreatePropertyType("weight", ValueType::Int64)._id;
    }

    void writeChain() {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declareMetadata(builder.getMetadata());

        LabelSet labelset;
        labelset.set(_stopLabel);

        for (size_t node = 0; node < edgeCount + 1; node++) {
            builder.addNode(labelset);
        }

        for (size_t edge = 0; edge < edgeCount; edge++) {
            const EdgeRecord& record = builder.addEdge(_roadType, NodeID {edge}, NodeID {edge + 1});

            builder.addEdgeProperty<types::Int64>(record, _weightID, weightOf(edge));
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    void writeWeightUpdates(const std::vector<EdgeWeight>& rewrites,
                            const std::vector<uint64_t>& nulls) {
        std::unique_ptr<Change> change = _graph->newChange();
        CommitBuilder* commit = change->access().getTip();
        DataPartBuilder& builder = commit->newBuilder();

        declareMetadata(builder.getMetadata());

        const GraphReader reader(commit->viewGraph());

        for (const EdgeWeight& rewrite : rewrites) {
            const EdgeRecord* record = reader.getEdge(EdgeID {rewrite.first});
            ASSERT_NE(record, nullptr);

            builder.addEdgeProperty<types::Int64>(*record, _weightID, rewrite.second);
        }

        for (const uint64_t edge : nulls) {
            const EdgeRecord* record = reader.getEdge(EdgeID {edge});
            ASSERT_NE(record, nullptr);

            builder.addEdgeProperty<types::Int64>(*record, _weightID, std::nullopt);
        }

        ASSERT_TRUE(change->access().submit(*_jobSystem));
    }

    // What the per-edge lookup resolves for every input edge: the newest part carrying
    // the property decides, and an edge nulled there holds no weight at all.
    void expectedWeights(const GraphView& view, std::vector<EdgeWeight>& rows) {
        const GraphReader reader(view);

        rows.clear();
        for (uint64_t edge = 0; edge < edgeCount; edge++) {
            const std::optional<const int64_t*> weight =
                reader.tryGetEdgeProperty<types::Int64>(_weightID, EdgeID {edge});
            const bool explicitNull = !weight.has_value();

            if (explicitNull || !weight.value()) {
                continue;
            }

            rows.emplace_back(edge, *weight.value());
        }
    }

    void collectByIterator(const GraphView& view, std::vector<EdgeWeight>& rows) {
        GetEdgePropertiesIterator<types::Int64> iterator(view, _weightID, &_inputEdges);

        rows.clear();
        for (; iterator.isValid(); iterator.next()) {
            rows.emplace_back(iterator.getCurrentEntityID().getValue(), iterator.get());
        }

        std::sort(rows.begin(), rows.end());
    }

    void collectByChunkWriter(const GraphView& view, std::vector<EdgeWeight>& rows) {
        ColumnVector<types::Int64::Primitive> weights;
        ColumnVector<size_t> indices;

        GetEdgePropertiesChunkWriter<types::Int64> writer(view, _weightID, &_inputEdges);
        writer.setOutput(&weights);
        writer.setIndices(&indices);

        writer.fill(std::numeric_limits<size_t>::max());
        ASSERT_EQ(weights.size(), indices.size());

        rows.clear();
        for (size_t row = 0; row < weights.size(); row++) {
            rows.emplace_back(_inputEdges[indices[row]].getValue(), weights[row]);
        }

        std::sort(rows.begin(), rows.end());
    }

    static bool holds(const std::vector<EdgeWeight>& rows, uint64_t edge, int64_t weight) {
        return std::find(rows.begin(), rows.end(), EdgeWeight {edge, weight}) != rows.end();
    }

    static bool mentions(const std::vector<EdgeWeight>& rows, uint64_t edge) {
        const auto isEdge = [edge](const EdgeWeight& row) { return row.first == edge; };
        return std::any_of(rows.begin(), rows.end(), isEdge);
    }

    std::unique_ptr<JobSystem> _jobSystem;
    std::unique_ptr<Graph> _graph;
    ColumnEdgeIDs _inputEdges;
    PropertyTypeID _weightID;
    LabelID _stopLabel;
    EdgeTypeID _roadType;
};

TEST_F(GetEdgePropertiesTest, expectationsPinTheRewriteAndTheNull) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeWeight> expected;
    expectedWeights(view, expected);

    EXPECT_EQ(expected.size(), edgeCount - 1);
    EXPECT_TRUE(holds(expected, rewrittenEdge, rewrittenWeight));
    EXPECT_FALSE(mentions(expected, nulledEdge));
}

TEST_F(GetEdgePropertiesTest, nulledEdgeIsNotEmittedWithItsOlderWeight) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeWeight> found;
    collectByIterator(view, found);

    EXPECT_FALSE(holds(found, nulledEdge, weightOf(nulledEdge)));
    EXPECT_FALSE(mentions(found, nulledEdge));
}

TEST_F(GetEdgePropertiesTest, chunkWriterDoesNotEmitTheNulledEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeWeight> found;
    collectByChunkWriter(view, found);

    EXPECT_FALSE(holds(found, nulledEdge, weightOf(nulledEdge)));
    EXPECT_FALSE(mentions(found, nulledEdge));
}

TEST_F(GetEdgePropertiesTest, iteratorYieldsOneCurrentWeightPerEdge) {
    const FrozenCommitTx transaction = _graph->openTransaction();
    const GraphView view = transaction.viewGraph();

    std::vector<EdgeWeight> expected;
    expectedWeights(view, expected);

    std::vector<EdgeWeight> found;
    collectByIterator(view, found);

    EXPECT_EQ(found, expected);
}
