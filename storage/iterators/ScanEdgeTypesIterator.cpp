#include "ScanEdgeTypesIterator.h"

#include "BioAssert.h"

namespace db {

ScanEdgeTypesIterator::ScanEdgeTypesIterator(const EdgeTypeMap& edgeTypeMap)
    : _it(edgeTypeMap.begin()),
    _end(edgeTypeMap.end())
{
}

ScanEdgeTypesIterator ScanEdgeTypesIterator::end(const EdgeTypeMap& edgeTypeMap) {
    return ScanEdgeTypesIterator(edgeTypeMap.end(), edgeTypeMap.end());
}

void ScanEdgeTypesIterator::reset() {
    bioassert(false, "Cannot reset ScanEdgeTypesIterator");
}

void ScanEdgeTypesIterator::next() {
    bioassert(isValid(), "ScanEdgeTypesIterator::next(): Iterator out of bounds");
    ++_it;
}

ScanEdgeTypesChunkWriter::ScanEdgeTypesChunkWriter(const EdgeTypeMap& edgeTypeMap)
    : ScanEdgeTypesIterator(edgeTypeMap)
{
}

void ScanEdgeTypesChunkWriter::fill(size_t maxCount) {
    bioassert(_ids || _names, "ScanEdgeTypesChunkWriter::fill(): Both columns are null");

    if (_ids) {
        _ids->clear();
    }

    if (_names) {
        _names->clear();
    }

    for (size_t i = 0; i < maxCount && isValid(); ++i) {
        if (_ids) {
            _ids->push_back(_it->_id);
        }

        if (_names) {
            _names->push_back(*_it->_name);
        }

        next();
    }
}

}
