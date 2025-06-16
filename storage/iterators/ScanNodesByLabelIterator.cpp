#include "ScanNodesByLabelIterator.h"

#include <numeric>

#include "DataPart.h"
#include "NodeContainer.h"

namespace db {

ScanNodesByLabelIterator::ScanNodesByLabelIterator(const GraphView& view,
                                                   const LabelSetHandle& labelset)
    : Iterator(view),
      _labelset(labelset)
{
    init();
}

ScanNodesByLabelIterator::~ScanNodesByLabelIterator() {
}

void ScanNodesByLabelIterator::init() {
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const NodeContainer& nodes = part->nodes();
        const auto& labelsetIndexer = nodes.getLabelSetIndexer();
        _labelsetIt = labelsetIndexer.matchIterate(_labelset);

        for (; _labelsetIt.isValid(); _labelsetIt.next()) {
            _range = nodes.getRange(_labelsetIt.getKey());
            _rangeIt = _range.begin();

            if (_rangeIt.isValid()) {
                return;
            }
        }
    }
}

void ScanNodesByLabelIterator::next() {
    ++_rangeIt;
    nextValid();
}

void ScanNodesByLabelIterator::nextValid() {
    while (!_rangeIt.isValid()) {
        _labelsetIt.next();

        while (!_labelsetIt.isValid()) {
            _partIt.next();

            if (!isValid()) {
                return;
            }

            const NodeContainer& nodes = _partIt.get()->nodes();
            const auto& labelsetIndexer = nodes.getLabelSetIndexer();
            _labelsetIt = labelsetIndexer.matchIterate(_labelset);
        }

        const DataPart* part = _partIt.get();
        const NodeContainer& nodes = part->nodes();

        _range = nodes.getRange(_labelsetIt.getKey());
        _rangeIt = _range.begin();

        if (!_rangeIt.isValid()) {
            continue;
        }
    }
}

ScanNodesByLabelChunkWriter::ScanNodesByLabelChunkWriter() = default;

ScanNodesByLabelChunkWriter::ScanNodesByLabelChunkWriter(const GraphView& view, const LabelSetHandle& labelset)
    : ScanNodesByLabelIterator(view, labelset)
{
}

void ScanNodesByLabelChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    msgbioassert(_nodeIDs, "ScanNodesByLabelChunkWriter must be initialized with a valid column");

    _nodeIDs->clear();

    while (isValid() && remainingToMax > 0) {
        // const auto* part = _partIt.get();
        const size_t currentOffset = (*_rangeIt - _range._first).getValue();
        const size_t rangeSize = std::min(_range._count - currentOffset, remainingToMax);
        const size_t prevSize = _nodeIDs->size();
        const size_t newSize = prevSize + rangeSize;
        _nodeIDs->resize(newSize);
        remainingToMax -= rangeSize;
        std::iota(_nodeIDs->begin() + prevSize, _nodeIDs->end(), *_rangeIt);
        _rangeIt += rangeSize;
        nextValid();
    }
}

}

