#include "ScanEdgePropertiesIterator.h"

#include "DataPart.h"
#include "IteratorUtils.h"
#include "iterators/TombstoneFilter.h"
#include "metadata/SupportedType.h"
#include "properties/PropertyManager.h"
#include "Panic.h"

namespace db {

template <SupportedType T>
ScanEdgePropertiesIterator<T>::ScanEdgePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID)
    : Iterator(view),
      _propTypeID(propTypeID) {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const PropertyManager& properties = _partIt.get()->edgeProperties();
        if (properties.hasPropertyType(_propTypeID)) {
            _props = properties.template all<T>(_propTypeID);
            _propIt = _props.begin();
            _currentID = properties.ids(_propTypeID).begin();
            return;
        }
    }
}

template <SupportedType T>
ScanEdgePropertiesIterator<T>::~ScanEdgePropertiesIterator() = default;

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::next() {
    _propIt++;
    _currentID++;
    nextValid();
}

template <SupportedType T>
const T::Primitive& ScanEdgePropertiesIterator<T>::get() const {
    return *_propIt;
}

template <SupportedType T>
bool ScanEdgePropertiesIterator<T>::nextDatapart() {
    _partIt.next();
    return _partIt.isNotEnd();
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::newPropertySpan() {
    const auto& properties = _partIt.get()->edgeProperties();

    _props = properties.template all<T>(_propTypeID);
    _propIt = _props.begin();
    _currentID = properties.ids(_propTypeID).begin();
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::nextValid() {
    while (_propIt == _props.end()) {
        _partIt.next();
        if (!_partIt.isNotEnd()) {
            return;
        }

        const DataPart* part = _partIt.get();
        if (part->edgeProperties().hasPropertyType(_propTypeID)) {
            newPropertySpan();
            return;
        }
    }
}

template <SupportedType T>
EdgeID ScanEdgePropertiesIterator<T>::getCurrentEdgeID() const {
    return _currentID->getValue();
}

template <SupportedType T>
ScanEdgePropertiesChunkWriter<T>::ScanEdgePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID)
    : ScanEdgePropertiesIterator<T>(view, propTypeID) {
}

template <SupportedType T>
void ScanEdgePropertiesChunkWriter<T>::filterTombstones() {
     // XXX: This should probably be an exception, as it will cause segfault if the
    // planner does not materialise the edge column
    msgbioassert(_edgeIDs,
                 "Attempted to filter the output of a ScanNodePropertiesChunkWriter "
                 "whilst not materialising the NodeID column.");

    TombstoneFilter filter(this->_view.tombstones());

    filter.populateDeletedIndices(*_edgeIDs);
    if (filter.empty()) {
        return;
    }

    filter.applyDeletedIndices(*_edgeIDs);
    if (_properties) {
        filter.applyDeletedIndices(*_properties);
    }
}

static constexpr size_t NColumns = 2;
static constexpr size_t NCombinations = 1 << NColumns;

template <SupportedType T>
void ScanEdgePropertiesChunkWriter<T>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    msgbioassert(_properties || _edgeIDs,
                 "ScanEdgePropertiesChunkWriter must be initialized with a valid column");
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    const auto getPrevSize = [&]() {
        if (_properties) {
            return _properties->size();
        }
        if (_edgeIDs) {
            return _edgeIDs->size();
        }
        panic("At least one column must be set");
    };

    if (_properties) {
        _properties->clear();
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
    }

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (this->isValid() && remainingToMax > 0) {
            const auto partOutEnd = this->_props.end();
            const size_t availInPart = std::distance(this->_propIt, partOutEnd);
            const size_t rangeSize = std::min(remainingToMax, availInPart);
            const size_t prevSize = getPrevSize();
            const size_t newSize = prevSize + rangeSize;

            if constexpr (conditions[0]) {
                this->_properties->resize(newSize);
            }
            if constexpr (conditions[1]) {
                this->_edgeIDs->resize(newSize);
            }
            remainingToMax -= rangeSize;

            for (size_t i = prevSize; i < newSize; i++) {
                if constexpr (conditions[0]) {
                    (*this->_properties)[i] = *this->_propIt;
                }
                if constexpr (conditions[1]) {
                    (*this->_edgeIDs)[i] = this->_currentID->getValue();
                }
                ++this->_propIt;
                ++this->_currentID;
            }
            this->nextValid();
        }
    };

    switch (bitmask::create(_properties, _edgeIDs)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
    }

    if (this->_view.tombstones().hasEdges()) {
        filterTombstones();
    }
}

template class ScanEdgePropertiesIterator<types::Int64>;
template class ScanEdgePropertiesIterator<types::UInt64>;
template class ScanEdgePropertiesIterator<types::Double>;
template class ScanEdgePropertiesIterator<types::String>;
template class ScanEdgePropertiesIterator<types::Bool>;

template class ScanEdgePropertiesChunkWriter<types::Int64>;
template class ScanEdgePropertiesChunkWriter<types::UInt64>;
template class ScanEdgePropertiesChunkWriter<types::Double>;
template class ScanEdgePropertiesChunkWriter<types::String>;
template class ScanEdgePropertiesChunkWriter<types::Bool>;

}

