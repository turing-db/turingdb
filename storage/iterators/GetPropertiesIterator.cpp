#include "GetPropertiesIterator.h"

#include "DataPart.h"
#include "properties/PropertyManager.h"

using namespace db;

template <PropertyIteratorClass IteratorClass, SupportedType T>
GetPropertiesIterator<IteratorClass, T>::GetPropertiesIterator(const GraphView& view,
                                                               PropertyTypeID propTypeID,
                                                               const ColumnIDs* inputEntityIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputEntityIDs(inputEntityIDs)
{
    // The initialization algorithm is the following:
    //   - For each part
    //   - If part has the requested property type
    //   - Loop on input node IDs and try to get the node's property
    //   - When a property is found, the iterator is initialized. Return

    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = part->nodeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _entityIt = _inputEntityIDs->cbegin();

            for (; _entityIt != _inputEntityIDs->cend(); _entityIt++) {
                _prop = properties.tryGet<T>(propTypeID, *_entityIt);

                if (_prop) {
                    return;
                }
            }
        }
    }
}

template <PropertyIteratorClass IteratorClass, SupportedType T>
void GetPropertiesIterator<IteratorClass, T>::next() {
    // Remmember that we loop on input node IDs for each DataPart,
    // so that we gather the info on a given node from all DataParts

    // Reset the _prop pointer
    _prop = nullptr;

    while (!_prop) {
        _entityIt++;

        // if no more inputEntityIDs, we can find the next valid DataPart
        // and start over the iteration on inputEntityIDs
        while (_entityIt == _inputEntityIDs->end()) {
            _partIt.next();

            if (!_partIt.isValid()) {
                // No more part, found all properties -> STOP
                return;
            }

            const DataPart* part = _partIt.get();
            const PropertyManager& properties = part->nodeProperties();

            if (!properties.hasPropertyType(_propTypeID)) {
                // Part does not have this property type, -> Next part
                continue;
            }

            // From here, the part has the requested property type
            // we break the while condition
            _entityIt = _inputEntityIDs->cbegin();
        }

        // From here, _partIt and _nodeIt are both valid
        const DataPart* part = _partIt.get();

        const PropertyManager& properties = (IteratorClass == PropertyIteratorClass::NODE)
            ? part->nodeProperties()
            : part->edgeProperties();

        _prop = properties.tryGet<T>(_propTypeID, *_entityIt);
        // If _prop is nullptr, the current nodeID does not have this prop,
        // -> start over the algorithm
        // Else, the outer while condition is broken,
        // -> we found the next valid prop
    }
}

namespace db {

template class GetPropertiesIterator<PropertyIteratorClass::NODE, types::Int64>;
template class GetPropertiesIterator<PropertyIteratorClass::NODE, types::UInt64>;
template class GetPropertiesIterator<PropertyIteratorClass::NODE, types::Double>;
template class GetPropertiesIterator<PropertyIteratorClass::NODE, types::String>;
template class GetPropertiesIterator<PropertyIteratorClass::NODE, types::Bool>;
template class GetPropertiesIterator<PropertyIteratorClass::EDGE, types::Int64>;
template class GetPropertiesIterator<PropertyIteratorClass::EDGE, types::UInt64>;
template class GetPropertiesIterator<PropertyIteratorClass::EDGE, types::Double>;
template class GetPropertiesIterator<PropertyIteratorClass::EDGE, types::String>;
template class GetPropertiesIterator<PropertyIteratorClass::EDGE, types::Bool>;

}

template <PropertyIteratorClass IteratorClass, SupportedType T>
GetPropertiesIteratorWithNull<IteratorClass, T>::GetPropertiesIteratorWithNull(const GraphView& view,
                                                                               PropertyTypeID propTypeID,
                                                                               const ColumnIDs* inputEntityIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputEntityIDs(inputEntityIDs)
{
    init();
}

template <PropertyIteratorClass IteratorClass, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorClass, T>::init() {
    if (_inputEntityIDs->empty()) {
        return;
    }

    _entityIt = _inputEntityIDs->cbegin();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = (IteratorClass == PropertyIteratorClass::NODE)
            ? part->nodeProperties()
            : part->edgeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, *_entityIt);
            if (_prop) {
                return;
            }
        }
    }
}

template <PropertyIteratorClass IteratorClass, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorClass, T>::reset() {
    Iterator::reset();
    _entityIt = _inputEntityIDs->cbegin();
    init();
}

template <PropertyIteratorClass IteratorClass, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorClass, T>::next() {
    // Reset the _prop pointer
    _prop = nullptr;

    _entityIt++;
    if (_entityIt == _inputEntityIDs->end()) {
        return;
    }

    Iterator::reset();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = (IteratorClass == PropertyIteratorClass::NODE)
            ? part->nodeProperties()
            : part->edgeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, *_entityIt);
            if (_prop) {
                return;
            }
        }
    }
}

namespace db {

template class GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, types::Int64>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, types::UInt64>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, types::Double>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, types::String>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, types::Bool>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, types::Int64>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, types::UInt64>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, types::Double>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, types::String>;
template class GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, types::Bool>;

}

template <PropertyIteratorClass IteratorClass, SupportedType T>
GetPropertiesWithNullChunkWriter<IteratorClass, T>::GetPropertiesWithNullChunkWriter(const GraphView& view,
                                                                                     PropertyTypeID propTypeID,
                                                                                     const ColumnIDs* inputEntityIDs)
    : GetPropertiesIteratorWithNull<IteratorClass, T>(view, propTypeID, inputEntityIDs)
{
}

template <PropertyIteratorClass IteratorClass, SupportedType T>
void GetPropertiesWithNullChunkWriter<IteratorClass, T>::fill(size_t maxCount) {
    auto& output = *_output;
    const size_t availableSize = std::distance(this->_entityIt, this->_inputEntityIDs->end());
    const size_t rangeSize = std::min(maxCount, availableSize);

    output.resize(rangeSize);
    for (size_t i = 0; i < rangeSize; i++) {
        const auto* currentProp = this->get();
        if (currentProp) {
            output[i] = *currentProp;
        } else {
            output[i] = {};
        }

        this->next();
    }
}

namespace db {

template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, types::Int64>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, types::Double>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, types::String>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, types::Bool>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, types::Int64>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, types::Double>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, types::String>;
template class GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, types::Bool>;

}
