#pragma once

#include "columns/ColumnVector.h"
#include "metadata/PropertyTypeMap.h"

namespace db {

class ScanPropertyTypesIterator {
public:
    ScanPropertyTypesIterator() = default;
    explicit ScanPropertyTypesIterator(const PropertyTypeMap& propertyTypeMap);

    static ScanPropertyTypesIterator end(const PropertyTypeMap& propertyTypeMap);

    void reset();
    void next();
    bool isValid() const { return _it != _end; }

    const PropertyTypeMap::Pair& get() const { return *_it; }
    PropertyTypeID getID() const { return _it->_pt._id; }
    ValueType getValueType() const { return _it->_pt._valueType; }
    std::string_view getName() const { return *_it->_name; }

    const PropertyTypeMap::Pair& operator*() const { return *_it; }

    ScanPropertyTypesIterator& operator++() {
        next();
        return *this;
    }

protected:
    PropertyTypeMap::Container::const_iterator _it;
    PropertyTypeMap::Container::const_iterator _end;

    ScanPropertyTypesIterator(PropertyTypeMap::Container::const_iterator begin,
                       PropertyTypeMap::Container::const_iterator end)
        : _it(begin),
        _end(end)
    {
    }
};

class ScanPropertyTypesChunkWriter : public ScanPropertyTypesIterator {
public:
    ScanPropertyTypesChunkWriter() = delete;
    explicit ScanPropertyTypesChunkWriter(const PropertyTypeMap& propertyTypeMap);

    void setPropertyTypes(ColumnVector<PropertyTypeID>* propertyTypes) { _ids = propertyTypes; }
    void setNames(ColumnVector<std::string_view>* names) { _names = names; }
    void setValueTypes(ColumnVector<ValueType>* valueTypes) { _valueTypes = valueTypes; }

    void fill(size_t maxCount);

private:
    ColumnVector<PropertyTypeID>* _ids {nullptr};
    ColumnVector<std::string_view>* _names {nullptr};
    ColumnVector<ValueType>* _valueTypes {nullptr};
};

struct ScanPropertyTypesRange {
    const PropertyTypeMap& _propertyTypeMap;

    ScanPropertyTypesIterator begin() const { return ScanPropertyTypesIterator {_propertyTypeMap}; }
    ScanPropertyTypesIterator end() const { return ScanPropertyTypesIterator::end(_propertyTypeMap); }
    ScanPropertyTypesChunkWriter chunkWriter() const { return ScanPropertyTypesChunkWriter {_propertyTypeMap}; }
};

}
