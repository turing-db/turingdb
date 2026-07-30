#pragma once

#include <optional>
#include <random>

#include "iterators/Iterator.h"

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "datapart/EdgeRecord.h"

namespace db {

class GraphView;

class NeighbourhoodSampleIterator : public Iterator {
public:
    NeighbourhoodSampleIterator(const GraphView& view, const ColumnNodeIDs* inputNodeIDs);
    ~NeighbourhoodSampleIterator() override = default;

    void next() final;

    void reset();

protected:
    const ColumnNodeIDs* _inputNodeIDs {nullptr};
    ColumnNodeIDs::ConstIterator _nodeIt;

    std::span<const EdgeRecord> _edges;
    std::span<const EdgeRecord>::iterator _edgeIt;

    void init();
    void nextValidForCurrentNode();
    void syncEdges();
};

class NeighbourhoodSampleChunkWriter final : public NeighbourhoodSampleIterator {
public:
    NeighbourhoodSampleChunkWriter(const GraphView& view,
                                   const ColumnNodeIDs* input,
                                   size_t sampleSize,
                                   std::optional<uint64_t> seed = std::nullopt);

    ~NeighbourhoodSampleChunkWriter() final = default;

    void fill(size_t maxCount);

    void setOutputColumns(ColumnNodeIDs* srcIDs,
                          ColumnEdgeIDs* edgeIDs,
                          ColumnEdgeTypes* edgeTypes,
                          ColumnNodeIDs* otherIDs);

    void setIndices(ColumnIndices* indices) { _indices = indices; }

private:
    ColumnNodeIDs* _srcIDs {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _otherIDs {nullptr};
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

}
