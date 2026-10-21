#include "ScanEdgesByEndpointLabelsChunkWriter.h"

#include "reader/GraphReader.h"
#include "IteratorUtils.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t NColumns = 4;
constexpr size_t NCombinations = 1 << NColumns;

}

template <typename IndexIterator>
ScanEdgesByEndpointLabelsChunkWriter<IndexIterator>::ScanEdgesByEndpointLabelsChunkWriter(const GraphView& view,
                                                                                          const LabelSetHandle& indexedLabels,
                                                                                          const LabelSetHandle& otherLabels)
    : IndexIterator(view, indexedLabels),
    _otherLabels(otherLabels),
    _filter(view)
{
}

template <typename IndexIterator>
ScanEdgesByEndpointLabelsChunkWriter<IndexIterator>::~ScanEdgesByEndpointLabelsChunkWriter() {
}

template <typename IndexIterator>
void ScanEdgesByEndpointLabelsChunkWriter<IndexIterator>::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);

    _filter.filter(_edgeIDs);

    if (_srcs) {
        _filter.filter(_srcs);
    }
    if (_tgts) {
        _filter.filter(_tgts);
    }
    if (_types) {
        _filter.filter(_types);
    }

    _filter.reset();
}

template <typename IndexIterator>
void ScanEdgesByEndpointLabelsChunkWriter<IndexIterator>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    bioassert(_srcs || _tgts || _edgeIDs || _types,
              "ScanEdgesByEndpointLabelsChunkWriter must be initialized with at least one valid column");

    if (_srcs) {
        _srcs->clear();
        _srcs->reserve(maxCount);
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
        _edgeIDs->reserve(maxCount);
    }
    if (_tgts) {
        _tgts->clear();
        _tgts->reserve(maxCount);
    }
    if (_types) {
        _types->clear();
        _types->reserve(maxCount);
    }

    const GraphReader reader(this->_view);

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (this->isValid() && remainingToMax > 0) {
            while (this->_edgeIt != this->_edges.end() && remainingToMax > 0) {
                const EdgeRecord& edge = *this->_edgeIt;
                const LabelSetHandle otherEndLabels = reader.getNodeLabelSet(edge._otherID);

                if (otherEndLabels.isValid() && otherEndLabels.hasAtLeastLabels(_otherLabels)) {
                    if constexpr (conditions[0]) {
                        _srcs->push_back(edge._nodeID);
                    }
                    if constexpr (conditions[1]) {
                        _edgeIDs->push_back(edge._edgeID);
                    }
                    if constexpr (conditions[2]) {
                        _tgts->push_back(edge._otherID);
                    }
                    if constexpr (conditions[3]) {
                        _types->push_back(edge._edgeTypeID);
                    }

                    remainingToMax--;
                }

                ++this->_edgeIt;
            }

            if (this->_edgeIt == this->_edges.end()) {
                this->nextValid();
            }
        }
    };

    switch (bitmask::create(_srcs, _edgeIDs, _tgts, _types)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
        CASE(4);
        CASE(5);
        CASE(6);
        CASE(7);
        CASE(8);
        CASE(9);
        CASE(10);
        CASE(11);
        CASE(12);
        CASE(13);
        CASE(14);
        CASE(15);
    }

    if (this->_view.hasDeletedEdges()) {
        filterTombstones();
    }
}

template class db::ScanEdgesByEndpointLabelsChunkWriter<ScanOutEdgesBySourceLabelIterator>;
template class db::ScanEdgesByEndpointLabelsChunkWriter<ScanInEdgesByTargetLabelIterator>;
