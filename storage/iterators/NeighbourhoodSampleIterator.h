#pragma once

#include <random>

#include "iterators/Iterator.h"

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"

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
                                   size_t sampleSize);

    ~NeighbourhoodSampleChunkWriter() final = default;


    void fill(size_t maxCount);

    void setOutputColumns(ColumnNodeIDs* srcIDs,
                          ColumnEdgeIDs* edgeIDs,
                          ColumnEdgeTypes* edgeTypes,
                          ColumnNodeIDs* otherIDs);

private:
    ColumnNodeIDs* _srcIDs {nullptr};
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnEdgeTypes* _edgeTypes {nullptr};
    ColumnNodeIDs* _otherIDs {nullptr};

    size_t _sampleSize {0};
    double _sampleRatio {1.0};

    std::mt19937 _generator {std::random_device {}()};
    std::uniform_int_distribution<> _replacementGenerator;

    // Uniform random sample from (0, 1)
    double rand01() {
        return ((_generator() >> 11U) + 0.5) * 0x1p-53;
    }

    size_t geometricSample(double W);

    size_t randomSampleOffset();
};

}
