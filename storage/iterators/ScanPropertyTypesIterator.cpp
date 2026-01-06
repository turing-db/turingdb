#include "ScanPropertyTypesIterator.h"

#include "BioAssert.h"

namespace db {

ScanPropertyTypesIterator::ScanPropertyTypesIterator(const PropertyTypeMap& propertyTypeMap)
    : _it(propertyTypeMap.begin()),
    _end(propertyTypeMap.end())
{
}

ScanPropertyTypesIterator ScanPropertyTypesIterator::end(const PropertyTypeMap& propertyTypeMap) {
    return ScanPropertyTypesIterator(propertyTypeMap.end(), propertyTypeMap.end());
}

void ScanPropertyTypesIterator::reset() {
    bioassert(false, "Cannot reset ScanPropertyTypesIterator");
}

void ScanPropertyTypesIterator::next() {
    bioassert(isValid(), "ScanPropertyTypesIterator::next(): Iterator out of bounds");
    ++_it;
}

ScanPropertyTypesChunkWriter::ScanPropertyTypesChunkWriter(const PropertyTypeMap& propertyTypeMap)
    : ScanPropertyTypesIterator(propertyTypeMap)
{
}

void ScanPropertyTypesChunkWriter::fill(size_t maxCount) {
    bioassert(_ids || _names || _valueTypes, "ScanPropertyTypesChunkWriter::fill(): All columns are null");

    if (_ids) {
        _ids->clear();
    }

    if (_valueTypes) {
        _valueTypes->clear();
    }

    if (_names) {
        _names->clear();
    }

    for (size_t i = 0; i < maxCount && isValid(); ++i) {
        if (_ids) {
            _ids->push_back(_it->_pt._id);
        }

        if (_valueTypes) {
            _valueTypes->push_back(_it->_pt._valueType);
        }

        if (_names) {
            _names->push_back(*_it->_name);
        }

        next();
    }
}

}
