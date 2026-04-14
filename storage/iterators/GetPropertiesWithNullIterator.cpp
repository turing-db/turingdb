#include "GetPropertiesWithNullIterator.h"

#include "DataPart.h"
#include "ID.h"
#include "iterators/Iterator.h"
#include "properties/PropertyManager.h"
#include "views/EntityPropertyView.h"

using namespace db;

template <IteratedID ID, SupportedType T>
GetPropertiesIteratorWithNull<ID, T>::GetPropertiesIteratorWithNull(const GraphView& view,
                                                                    PropertyTypeID propTypeID,
                                                                    const ColumnIDs* inputIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputIDs(inputIDs)
{
    init();
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIteratorWithNull<ID, T>::init() {
    if (_inputIDs->empty()) {
        return;
    }

    _entityIt = _inputIDs->cbegin();
    EntityID id {_entityIt->getValue()};

    _partIt.skipToEnd();
    while (_partIt.isNotStart()) {
        _partIt.prev();
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        if (!properties.hasPropertyType(_propTypeID)) {
            continue;
        }

        const auto maybeProp = properties.tryGetWithNull<T>(_propTypeID, id);

        const bool explicitNull = !maybeProp.has_value();
        if (explicitNull) {
            return;
        }

        _prop = maybeProp.value();
        if (_prop) {
            return;
        }
    }
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIteratorWithNull<ID, T>::reset() {
    _partIt.skipToEnd();
    _entityIt = _inputIDs->cbegin();
    init();
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIteratorWithNull<ID, T>::next() {
    // Reset the _prop pointer
    _prop = nullptr;

    _entityIt++;
    if (_entityIt == _inputIDs->end()) {
        return;
    }

    EntityID id = _entityIt->getValue();

    // Iterate over parts in reverse order: find the most recent property value first
    _partIt.skipToEnd();
    while (_partIt.isNotStart()) {
        _partIt.prev();
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        if (!properties.hasPropertyType(_propTypeID)){
            continue;
        }

        const auto maybeProp = properties.tryGetWithNull<T>(_propTypeID, id);

        const bool explicitNull = !maybeProp.has_value();

        // nullopt => null valued => break; returning nullptr
        if (explicitNull) {
            return;
        }

        _prop = maybeProp.value();
        if (_prop) { // not nullptr => return the value
            return;
        }
        // nullptr => check next datapart
    }
}

namespace db {

template class GetPropertiesIteratorWithNull<NodeID, types::Int64>;
template class GetPropertiesIteratorWithNull<NodeID, types::UInt64>;
template class GetPropertiesIteratorWithNull<NodeID, types::Double>;
template class GetPropertiesIteratorWithNull<NodeID, types::String>;
template class GetPropertiesIteratorWithNull<NodeID, types::Bool>;
template class GetPropertiesIteratorWithNull<NodeID, types::Embedding>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Int64>;
template class GetPropertiesIteratorWithNull<EdgeID, types::UInt64>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Double>;
template class GetPropertiesIteratorWithNull<EdgeID, types::String>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Bool>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Embedding>;

}

template <IteratedID ID, SupportedType T>
GetPropertiesWithNullChunkWriter<ID, T>::GetPropertiesWithNullChunkWriter(const GraphView& view,
                                                                          PropertyTypeID propTypeID,
                                                                          const ColumnIDs* inputIDs)
    : GetPropertiesIteratorWithNull<ID, T>(view, propTypeID, inputIDs)
{
}

template <IteratedID ID, SupportedType T>
void GetPropertiesWithNullChunkWriter<ID, T>::fill(size_t maxCount) {
    auto& output = *_output;
    const size_t availableSize = std::distance(this->_entityIt, this->_inputIDs->end());
    const size_t rangeSize = std::min(maxCount, availableSize);

    output.resize(rangeSize);
    for (size_t i = 0; i < rangeSize; i++) {
        output[i] = this->get();

        this->next();
    }
}

namespace db {

template class GetPropertiesWithNullChunkWriter<NodeID, types::Int64>;
template class GetPropertiesWithNullChunkWriter<NodeID, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<NodeID, types::Double>;
template class GetPropertiesWithNullChunkWriter<NodeID, types::String>;
template class GetPropertiesWithNullChunkWriter<NodeID, types::Bool>;
template class GetPropertiesWithNullChunkWriter<NodeID, types::Embedding>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Int64>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Double>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::String>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Bool>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Embedding>;

}
