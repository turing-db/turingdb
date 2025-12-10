#include "ScanLabelsIterator.h"

#include "BioAssert.h"

namespace db {

ScanLabelsIterator::ScanLabelsIterator(const LabelMap& labelMap)
    : _it(labelMap.begin()),
    _end(labelMap.end())
{
}

ScanLabelsIterator ScanLabelsIterator::end(const LabelMap& labelMap) {
    return ScanLabelsIterator(labelMap.end(), labelMap.end());
}

void ScanLabelsIterator::reset() {
    bioassert(false, "Cannot reset ScanLabelsIterator");
}

void ScanLabelsIterator::next() {
    bioassert(isValid(), "ScanLabelsIterator::next(): Iterator out of bounds");
    ++_it;
}

ScanLabelsChunkWriter::ScanLabelsChunkWriter(const LabelMap& labelMap)
    : ScanLabelsIterator(labelMap)
{
}

void ScanLabelsChunkWriter::fill(size_t maxCount) {
    bioassert(_ids || _names, "ScanLabelsChunkWriter::fill(): Both columns are null");

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
