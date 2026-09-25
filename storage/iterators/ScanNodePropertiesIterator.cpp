#include "ScanNodePropertiesIterator.h"

#include "datapart/DataPart.h"
#include "IteratorUtils.h"
#include "properties/PropertyManager.h"

#include "BioAssert.h"

namespace db {

template <SupportedType T>
ScanNodePropertiesIterator<T>::ScanNodePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID)
    : Iterator(view),
     _propTypeID(propTypeID)
{
    init();
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const PropertyManager& properties = _partIt.get()->nodeProperties();
        if (properties.hasPropertyType(_propTypeID)) {
            newPropertySpan();
            skipOverridden();

            if (_propIt != _props.end()) {
                return;
            }
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
    skipOverridden();

    while (_propIt == _props.end()) {
        _partIt.next();
        if (!_partIt.isNotEnd()) {
            return;
        }

        const DataPart* part = _partIt.get();
        if (part->nodeProperties().hasPropertyType(_propTypeID)) {
            newPropertySpan();
            skipOverridden();
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

    collectNewerContainers();
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::collectNewerContainers() {
    _newerContainers.clear();

    PartIterator newerIt = _partIt;

    for (newerIt.next(); newerIt.isNotEnd(); newerIt.next()) {
        const PropertyManager& nodeProperties = newerIt.get()->nodeProperties();
        const TypedPropertyContainer<T>* container = nodeProperties.tryGetContainer<T>(_propTypeID);

        if (container) {
            _newerContainers.push_back(container);
        }
    }
}

template <SupportedType T>
void ScanNodePropertiesIterator<T>::skipOverridden() {
    if (_newerContainers.empty()) {
        return;
    }

    while (_propIt != _props.end() && isOverridden(*_currentID)) {
        _propIt++;
        _currentID++;
    }
}

// hasEntry, not has: a newer part holding an explicit null still answers for the entity,
// and must drop the value this part stores rather than let it stand.
template <SupportedType T>
bool ScanNodePropertiesIterator<T>::isOverridden(EntityID entityID) const {
    for (const PropertyContainer* container : _newerContainers) {
        if (container->hasEntry(entityID)) {
            return true;
        }
    }

    return false;
}

template <SupportedType T>
ScanNodePropertiesChunkWriter<T>::ScanNodePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID)
    : ScanNodePropertiesIterator<T>(view, propTypeID),
    _filter(view)
{
}

template <SupportedType T>
void ScanNodePropertiesChunkWriter<T>::filterTombstones() {
    // Base column of this ChunkWriter is _nodeIDs
    _filter.populateRanges(_nodeIDs);

    _filter.filter(_nodeIDs);

    if (_properties) {
        _filter.filter(_properties);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 2;
static constexpr size_t NCombinations = 1 << NColumns;

template <SupportedType T>
void ScanNodePropertiesChunkWriter<T>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    bioassert(_properties || _nodeIDs,
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
            const size_t previousSize = size;

            for (size_t row = 0; row < rangeSize; row++) {
                if (!this->isOverridden(*this->_currentID)) {
                    if constexpr (conditions[0]) {
                        (*this->_properties)[size] = *this->_propIt;
                    }
                    if constexpr (conditions[1]) {
                        (*this->_nodeIDs)[size] = NodeID {this->_currentID->getValue()};
                    }
                    size++;
                }

                ++this->_propIt;
                ++this->_currentID;
            }

            remainingToMax -= size - previousSize;

            this->nextValid();
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

    if (this->_view.hasDeletedNodes()) {
        filterTombstones();
    }
}

template class ScanNodePropertiesIterator<types::Int64>;
template class ScanNodePropertiesIterator<types::UInt64>;
template class ScanNodePropertiesIterator<types::Double>;
template class ScanNodePropertiesIterator<types::String>;
template class ScanNodePropertiesIterator<types::Bool>;
template class ScanNodePropertiesIterator<types::Embedding>;
template class ScanNodePropertiesIterator<types::List>;
template class ScanNodePropertiesIterator<types::DateTime>;
template class ScanNodePropertiesIterator<types::Map>;

template class ScanNodePropertiesChunkWriter<types::Int64>;
template class ScanNodePropertiesChunkWriter<types::UInt64>;
template class ScanNodePropertiesChunkWriter<types::Double>;
template class ScanNodePropertiesChunkWriter<types::String>;
template class ScanNodePropertiesChunkWriter<types::Bool>;
template class ScanNodePropertiesChunkWriter<types::Embedding>;
template class ScanNodePropertiesChunkWriter<types::List>;
template class ScanNodePropertiesChunkWriter<types::DateTime>;
template class ScanNodePropertiesChunkWriter<types::Map>;

}

