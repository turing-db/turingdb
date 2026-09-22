#pragma once

#include <optional>
#include <random>

#include "iterators/Iterator.h"

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"
#include "columns/ColumnOptVector.h"

#include "datapart/EdgeRecord.h"

namespace db {

class GraphView;

/**
 * @brief Tombstone-aware iterator over the in edges of each NodeID in @ref _inputNodeIDs
 * @detail Traverses each DataPart for every NodeID, as opposed to GetOutEdgesIterator
 */
class NeighbourhoodSampleIterator : public Iterator {
public:
    NeighbourhoodSampleIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);
    ~NeighbourhoodSampleIterator() override = default;

    void next() final;

    void reset();

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};

    size_t _nodeIndex {0};

    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void initFrom(size_t index);
    void nextValidForCurrentNode();
    void syncEdges();
    bool deleted(const EdgeRecord& e) const;
};

/**
 * @brief Uniform random sample of the in edges of each NodeID in @ref _inputNodeIDs
 */
template <typename NodeColumn>
class NeighbourhoodSampleChunkWriter final : public NeighbourhoodSampleIterator {
public:
    NeighbourhoodSampleChunkWriter(const GraphView& view,
                                   const ColumnNodeIDs* input,
                                   size_t sampleSize,
                                   std::optional<uint64_t> seed = std::nullopt);

    ~NeighbourhoodSampleChunkWriter() final = default;

    void fill(size_t maxCount);

    bool isDone() const { return _nodeIndex == _inputNodeIDs->size(); }

    size_t getSampleSize() const { return _sampleSize; }

    void setOutputColumns(NodeColumn* srcIDs,
                          ColumnEdgeIDs* edgeIDs,
                          ColumnEdgeTypes* edgeTypes,
                          NodeColumn* otherIDs);

    void setIndices(ColumnIndices* indices) { _indices = indices; }

private:
    NodeColumn* _srcIDs {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    NodeColumn* _otherIDs {nullptr};
    ColumnIndices* _indices {nullptr};

    size_t _sampleSize {0};
    double _sampleRatio {1.0};

    std::mt19937_64 _generator;
    std::uniform_int_distribution<> _replacementGenerator;

    // Uniform random sample from (0, 1)
    double rand01() {
        return ((_generator() >> 12U) + 0.5) * 0x1p-52;
    }

    size_t geometricSample(double W);

    size_t randomSampleOffset();
};

using NeighbourhoodSampleWriter = NeighbourhoodSampleChunkWriter<ColumnNodeIDs>;
using NullableNeighbourhoodSampleWriter = NeighbourhoodSampleChunkWriter<ColumnOptVector<NodeID>>;

}
