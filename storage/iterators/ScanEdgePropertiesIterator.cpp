#include "ScanEdgePropertiesIterator.h"

#include "datapart/DataPart.h"
#include "IteratorUtils.h"
#include "metadata/SupportedType.h"
#include "properties/PropertyManager.h"

namespace db {

template <SupportedType T>
ScanEdgePropertiesIterator<T>::ScanEdgePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID)
    : Iterator(view),
    _propTypeID(propTypeID)
{
    init();
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const PropertyManager& properties = _partIt.get()->edgeProperties();
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

    collectNewerContainers();
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::collectNewerContainers() {
    _newerContainers.clear();

    PartIterator newerIt = _partIt;

    for (newerIt.next(); newerIt.isNotEnd(); newerIt.next()) {
        const PropertyManager& edgeProperties = newerIt.get()->edgeProperties();
        const TypedPropertyContainer<T>* container = edgeProperties.tryGetContainer<T>(_propTypeID);

        if (container) {
            _newerContainers.push_back(container);
        }
    }
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::skipOverridden() {
    if (_newerContainers.empty()) {
        return;
    }

    while (_propIt != _props.end() && isOverridden(*_currentID)) {
        _propIt++;
        _currentID++;
    }
}

template <SupportedType T>
bool ScanEdgePropertiesIterator<T>::isOverridden(EntityID entityID) const {
    for (const PropertyContainer* container : _newerContainers) {
        if (container->hasEntry(entityID)) {
            return true;
        }
    }

    return false;
}

template <SupportedType T>
void ScanEdgePropertiesIterator<T>::nextValid() {
    skipOverridden();

    while (_propIt == _props.end()) {
        _partIt.next();
        if (!_partIt.isNotEnd()) {
            return;
        }

        const DataPart* part = _partIt.get();
        if (part->edgeProperties().hasPropertyType(_propTypeID)) {
            newPropertySpan();
            skipOverridden();
        }
    }
}

template <SupportedType T>
EdgeID ScanEdgePropertiesIterator<T>::getCurrentEdgeID() const {
    return _currentID->getValue();
}

template <SupportedType T>
ScanEdgePropertiesChunkWriter<T>::ScanEdgePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID)
    : ScanEdgePropertiesIterator<T>(view, propTypeID),
    _filter(view)
{
}

template <SupportedType T>
void ScanEdgePropertiesChunkWriter<T>::filterTombstones() {
    // Base column of this ChunkWriter is _edgeIDs
    _filter.populateRanges(_edgeIDs);
    _filter.filter(_edgeIDs);

    if (_properties ) {
        _filter.filter(_properties);
    }

    _filter.reset();
}

static constexpr size_t NColumns = 2;
static constexpr size_t NCombinations = 1 << NColumns;

template <SupportedType T>
void ScanEdgePropertiesChunkWriter<T>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    bioassert(_properties || _edgeIDs,
              "ScanEdgePropertiesChunkWriter must be initialized with a valid column");
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    if (_properties) {
        _properties->clear();
    }
    if (_edgeIDs) {
        _edgeIDs->clear();
    }

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
                if (this->_edgeIDs->size() < newSize) {
                    this->_edgeIDs->resize(newSize);
                }
            }
            const size_t previousSize = size;

            for (size_t row = 0; row < rangeSize; row++) {
                if (!this->isOverridden(*this->_currentID)) {
                    if constexpr (conditions[0]) {
                        (*this->_properties)[size] = *this->_propIt;
                    }
                    if constexpr (conditions[1]) {
                        (*this->_edgeIDs)[size] = this->_currentID->getValue();
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
            this->_edgeIDs->resize(size);
        }
    };

    switch (bitmask::create(_properties, _edgeIDs)) {
        CASE(0);
        CASE(1);
        CASE(2);
        CASE(3);
    }

    if (this->_view.hasDeletedEdges()) {
        filterTombstones();
    }
}

template class ScanEdgePropertiesIterator<types::Int64>;
template class ScanEdgePropertiesIterator<types::UInt64>;
template class ScanEdgePropertiesIterator<types::Double>;
template class ScanEdgePropertiesIterator<types::String>;
template class ScanEdgePropertiesIterator<types::Bool>;
template class ScanEdgePropertiesIterator<types::Embedding>;
template class ScanEdgePropertiesIterator<types::List>;
template class ScanEdgePropertiesIterator<types::DateTime>;
template class ScanEdgePropertiesIterator<types::Map>;

template class ScanEdgePropertiesChunkWriter<types::Int64>;
template class ScanEdgePropertiesChunkWriter<types::UInt64>;
template class ScanEdgePropertiesChunkWriter<types::Double>;
template class ScanEdgePropertiesChunkWriter<types::String>;
template class ScanEdgePropertiesChunkWriter<types::Bool>;
template class ScanEdgePropertiesChunkWriter<types::Embedding>;
template class ScanEdgePropertiesChunkWriter<types::List>;
template class ScanEdgePropertiesChunkWriter<types::DateTime>;
template class ScanEdgePropertiesChunkWriter<types::Map>;

}

