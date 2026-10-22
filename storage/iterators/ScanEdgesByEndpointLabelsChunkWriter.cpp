#include "ScanEdgesByEndpointLabelsChunkWriter.h"

#include <algorithm>

#include "reader/GraphReader.h"
#include "IteratorUtils.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t N_COLUMNS = 4;
constexpr size_t N_COMBINATIONS = 1 << N_COLUMNS;

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
    static constexpr auto bools = generateArray<N_COLUMNS, N_COMBINATIONS>();
    static constexpr auto masks = generateBitmasks<N_COLUMNS, N_COMBINATIONS>();

    bioassert(_srcs || _tgts || _edgeIDs || _types,
              "ScanEdgesByEndpointLabelsChunkWriter must be initialized with at least one valid column");

    if (_srcs) {
        _srcs->clear();
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
    }
    if (_tgts) {
        _tgts->clear();
    }
    if (_types) {
        _types->clear();
    }

    const GraphReader reader(this->_view);
    size_t written = 0;

    const auto fill = [&]<std::array<bool, N_COLUMNS> conditions>() {
        while (this->isValid() && remainingToMax > 0) {
            _keptEdges.clear();
            while (this->_edgeIt != this->_edges.end() && _keptEdges.size() < remainingToMax) {
                const EdgeRecord& edge = *this->_edgeIt;
                const LabelSetHandle otherEndLabels = reader.getNodeLabelSet(edge._otherID);

                if (otherEndLabels.isValid() && otherEndLabels.hasAtLeastLabels(_otherLabels)) {
                    _keptEdges.push_back(&edge);
                }

                ++this->_edgeIt;
            }

            const size_t keptCount = _keptEdges.size();
            const size_t newSize = written + keptCount;

            if constexpr (conditions[0]) {
                _srcs->resize(newSize);
                std::transform(_keptEdges.begin(),
                               _keptEdges.end(),
                               _srcs->begin() + written,
                               [](const EdgeRecord* edge) { return edge->_nodeID; });
            }
            if constexpr (conditions[1]) {
                _edgeIDs->resize(newSize);
                std::transform(_keptEdges.begin(),
                               _keptEdges.end(),
                               _edgeIDs->begin() + written,
                               [](const EdgeRecord* edge) { return edge->_edgeID; });
            }
            if constexpr (conditions[2]) {
                _tgts->resize(newSize);
                std::transform(_keptEdges.begin(),
                               _keptEdges.end(),
                               _tgts->begin() + written,
                               [](const EdgeRecord* edge) { return edge->_otherID; });
            }
            if constexpr (conditions[3]) {
                _types->resize(newSize);
                std::transform(_keptEdges.begin(),
                               _keptEdges.end(),
                               _types->begin() + written,
                               [](const EdgeRecord* edge) { return edge->_edgeTypeID; });
            }

            written = newSize;
            remainingToMax -= keptCount;

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
