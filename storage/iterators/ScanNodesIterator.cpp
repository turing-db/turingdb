#include "ScanNodesIterator.h"

#include <algorithm>
#include <numeric>

#include "BioAssert.h"
#include "DataPart.h"
#include "Iterator.h"

using namespace db;

ScanNodesIterator::ScanNodesIterator(const GraphView& view)
    : Iterator(view)
{
    init();
}

ScanNodesIterator::~ScanNodesIterator() {
}

void ScanNodesIterator::init() {
    // Search the first data part that has nodes
    _partIt.skipEmptyParts();

    if (_partIt.isValid()) {
        const DataPart* part = _partIt.get();
        _currentNodeID = part->getFirstNodeID();
        _partEnd = _currentNodeID + part->getNodeCount();
    }
}

void ScanNodesIterator::reset() {
    Iterator::reset();
    init();
}

void ScanNodesIterator::fill(ColumnNodeIDs* column, size_t maxNodes) {
    size_t remainingToMax = maxNodes;

    while (isValid() && remainingToMax > 0) {
        const size_t availableInPart = _partEnd.getValue() - _currentNodeID.getValue();
        const size_t rangeSize = std::min(maxNodes, availableInPart);
        const size_t prevSize = column->size();
        const size_t newSize = prevSize + rangeSize;
        column->resize(newSize);
        std::iota(column->begin() + prevSize, column->end(), _currentNodeID);
        _currentNodeID += rangeSize;
        remainingToMax -= rangeSize;

        // Go to next part
        if (_currentNodeID >= _partEnd) {
            next();
        }
    }
}

void ScanNodesIterator::next() {
    _currentNodeID++;
    nextValid();
}

void ScanNodesIterator::nextValid() {
    while (_currentNodeID >= _partEnd) {
        ++_partIt;

        if (!_partIt.isValid()) {
            return;
        }

        const DataPart* part = _partIt.get();
        _currentNodeID = part->getFirstNodeID();
        _partEnd = _currentNodeID + part->getNodeCount();
    }
}

ScanNodesChunkWriter::ScanNodesChunkWriter() = default;

ScanNodesChunkWriter::ScanNodesChunkWriter(const GraphView& view)
    : ScanNodesIterator(view)
{
}

void ScanNodesChunkWriter::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    msgbioassert(_nodeIDs, "ScanNodesChunkWriter must be initialized with a valid column");
    _nodeIDs->clear();

    while (isValid() && remainingToMax > 0) {
        const auto* part = _partIt.get();
        const auto partLastNode = part->getFirstNodeID() + part->getNodeCount();
        const size_t availInPart = partLastNode.getValue() - _currentNodeID.getValue();
        const size_t rangeSize = std::min(remainingToMax, availInPart);
        const size_t prevSize = _nodeIDs->size();
        const size_t newSize = prevSize + rangeSize;

        _nodeIDs->resize(newSize);
        std::iota(_nodeIDs->begin() + prevSize, _nodeIDs->end(), _currentNodeID);
        _currentNodeID += rangeSize;
        remainingToMax -= rangeSize;
        nextValid();
    }
}

