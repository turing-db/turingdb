#include "ScanNodePropertiesIterator.h"

#include "DataPart.h"
#include "IteratorUtils.h"
#include "iterators/TombstoneFilter.h"
#include "properties/PropertyManager.h"
#include "BioAssert.h"

namespace db {

template <SupportedType T>
ScanNodePropertiesIterator<T>::ScanNodePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID)
    : Iterator(view),
      _propTypeID(propTypeID) {
    init();
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const PropertyManager& properties = _partIt.get()->nodeProperties();
        if (properties.hasPropertyType(_propTypeID)) {
            _props = properties.template all<T>(_propTypeID);
            _propIt = _props.begin();
            _currentID = properties.ids(_propTypeID).begin();
            return;
        }
    }
}

template <SupportedType T>
ScanNodePropertiesIterator<T>::~ScanNodePropertiesIterator() = default;

template <SupportedType T>
void ScanNodePropertiesIterator<T>::reset() {
    Iterator::reset();
    init();
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::next() {
    _propIt++;
    _currentID++;
    nextValid();
}

template <SupportedType T>
const T::Primitive& ScanNodePropertiesIterator<T>::get() const {
    return *_propIt;
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::nextValid() {
    while (_propIt == _props.end()) {
        _partIt.next();
        if (!_partIt.isNotEnd()) {
            return;
        }

        const DataPart* part = _partIt.get();
        if (part->nodeProperties().hasPropertyType(_propTypeID)) {
            newPropertySpan();
            return;
        }
    }
}

template <SupportedType T>
bool ScanNodePropertiesIterator<T>::nextDatapart() {
    _partIt.next();
    return _partIt.isNotEnd();
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::newPropertySpan() {
    const auto& properties = _partIt.get()->nodeProperties();

    _props = properties.template all<T>(_propTypeID);
    _propIt = _props.begin();
    _currentID = properties.ids(_propTypeID).begin();
}

template <SupportedType T>
ScanNodePropertiesChunkWriter<T>::ScanNodePropertiesChunkWriter() = default;

template <SupportedType T>
ScanNodePropertiesChunkWriter<T>::ScanNodePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID)
    : ScanNodePropertiesIterator<T>(view, propTypeID) {
}

template <SupportedType T>
void ScanNodePropertiesChunkWriter<T>::filterTombstones() {
    // XXX: This should probably be an exception, as it will cause segfault if the
    // planner does not materialise the node column
    msgbioassert(_nodeIDs,
                 "Attempted to filter the output of a ScanNodePropertiesChunkWriter "
                 "whilst not materialising the NodeID column.");

    TombstoneFilter filter(this->_view.tombstones());

    filter.populateDeletedIndices(*_nodeIDs);

    if (filter.empty()) {
        return;
    }

    filter.applyDeletedIndices(*_nodeIDs);
    if (_properties) {
        filter.applyDeletedIndices(*_properties);
    }
}

static constexpr size_t NColumns = 2;
static constexpr size_t NCombinations = 1 << NColumns;

template <SupportedType T>
void ScanNodePropertiesChunkWriter<T>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    msgbioassert(_properties || _nodeIDs,
                 "ScanNodePropertiesChunkWriter must be initialized with a valid column");
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        size_t size = 0;
        while (this->isValid() && remainingToMax > 0) {
            const size_t availInPart = std::distance(this->_propIt, this->_props.end());
            const size_t rangeSize = std::min(remainingToMax, availInPart);
            const size_t newSize = size + rangeSize;

            if constexpr (conditions[0]) {
                if (this->_properties->size() < newSize) {
                    this->_properties->resize(newSize);
                }
            }
            if constexpr (conditions[1]) {
                if (this->_nodeIDs->size() < newSize) {
                    this->_nodeIDs->resize(newSize);
                }
            }
            remainingToMax -= rangeSize;

            if constexpr (conditions[0]) {
                auto& properties = *this->_properties;
                auto& propIt = this->_propIt;
                for (size_t i = size; i < newSize; i++) {
                    properties[i] = *propIt;
                    ++propIt;
                }
            }
            if constexpr (conditions[1]) {
                auto& nodeIDs = *this->_nodeIDs;
                auto& currentID = this->_currentID;
                for (size_t i = size; i < newSize; i++) {
                    nodeIDs[i] = NodeID {this->_currentID->getValue()};
                    ++currentID++;
                }
            }
            this->nextValid();
            size = newSize;
        }

        if constexpr (conditions[0]) {
            this->_properties->resize(size);
        }
        if constexpr (conditions[1]) {
            this->_nodeIDs->resize(size);
        }
    };

    switch (bitmask::create(_properties, _nodeIDs)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
    }

    if (this->_view.tombstones().hasNodes()) {
        filterTombstones();
    }
}


template class ScanNodePropertiesIterator<types::Int64>;
template class ScanNodePropertiesIterator<types::UInt64>;
template class ScanNodePropertiesIterator<types::Double>;
template class ScanNodePropertiesIterator<types::String>;
template class ScanNodePropertiesIterator<types::Bool>;

template class ScanNodePropertiesChunkWriter<types::Int64>;
template class ScanNodePropertiesChunkWriter<types::UInt64>;
template class ScanNodePropertiesChunkWriter<types::Double>;
template class ScanNodePropertiesChunkWriter<types::String>;
template class ScanNodePropertiesChunkWriter<types::Bool>;

}

