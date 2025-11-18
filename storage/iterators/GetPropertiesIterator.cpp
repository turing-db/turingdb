#include "GetPropertiesIterator.h"

#include "DataPart.h"
#include "properties/PropertyManager.h"

using namespace db;

template <IteratedID ID, SupportedType T>
GetPropertiesIterator<ID, T>::GetPropertiesIterator(const GraphView& view,
                                                    PropertyTypeID propTypeID,
                                                    const ColumnIDs* inputIDs)
    : Iterator(view),
    _propTypeID(propTypeID),
    _inputIDs(inputIDs)

{
    init();
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIterator<ID, T>::init() {
    // The initialization algorithm is the following:
    //   - For each part
    //   - If part has the requested property type
    //   - Loop on input node IDs and try to get the node's property
    //   - When a property is found, the iterator is initialized. Return

    for (; _partIt.isNotEnd(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = (std::is_same_v<ID, NodeID>)
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _entityIt = _inputIDs->cbegin();

            for (; _entityIt != _inputIDs->cend(); _entityIt++) {
                _prop = properties.tryGet<T>(_propTypeID, _entityIt->getValue());

                if (_prop) {
                    return;
                }
            }
        }
    }
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIterator<ID, T>::reset() {
    Iterator::reset();
    init();
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIterator<ID, T>::next() {
    // Remmember that we loop on input node IDs for each DataPart,
    // so that we gather the info on a given node from all DataParts

    // Reset the _prop pointer
    _prop = nullptr;

    while (!_prop) {
        _entityIt++;

        // if no more inputIDs, we can find the next valid DataPart
        // and start over the iteration on inputIDs
        while (_entityIt == _inputIDs->end()) {
            _partIt.next();

            if (!_partIt.isNotEnd()) {
                // No more part, found all properties -> STOP
                return;
            }

            const DataPart* part = _partIt.get();
            const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                                  ? part->nodeProperties()
                                                  : part->edgeProperties();

            if (!properties.hasPropertyType(_propTypeID)) {
                // Part does not have this property type, -> Next part
                continue;
            }

            // From here, the part has the requested property type
            // we break the while condition
            _entityIt = _inputIDs->cbegin();
        }

        // From here, _partIt and _nodeIt are both valid
        const DataPart* part = _partIt.get();

        const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        _prop = properties.tryGet<T>(_propTypeID, _entityIt->getValue());
        // If _prop is nullptr, the current nodeID does not have this prop,
        // -> start over the algorithm
        // Else, the outer while condition is broken,
        // -> we found the next valid prop
    }
}

namespace db {

template class GetPropertiesIterator<NodeID, types::Int64>;
template class GetPropertiesIterator<NodeID, types::UInt64>;
template class GetPropertiesIterator<NodeID, types::Double>;
template class GetPropertiesIterator<NodeID, types::String>;
template class GetPropertiesIterator<NodeID, types::Bool>;
template class GetPropertiesIterator<EdgeID, types::Int64>;
template class GetPropertiesIterator<EdgeID, types::UInt64>;
template class GetPropertiesIterator<EdgeID, types::Double>;
template class GetPropertiesIterator<EdgeID, types::String>;
template class GetPropertiesIterator<EdgeID, types::Bool>;

}

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
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, _entityIt->getValue());
            if (_prop) {
                return;
            }
        }
    }
}

template <IteratedID ID, SupportedType T>
void GetPropertiesIteratorWithNull<ID, T>::reset() {
    Iterator::reset();
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

    Iterator::reset();
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = std::is_same_v<ID, NodeID>
                                              ? part->nodeProperties()
                                              : part->edgeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, _entityIt->getValue());
            if (_prop) {
                return;
            }
        }
    }
}

namespace db {

template class GetPropertiesIteratorWithNull<NodeID, types::Int64>;
template class GetPropertiesIteratorWithNull<NodeID, types::UInt64>;
template class GetPropertiesIteratorWithNull<NodeID, types::Double>;
template class GetPropertiesIteratorWithNull<NodeID, types::String>;
template class GetPropertiesIteratorWithNull<NodeID, types::Bool>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Int64>;
template class GetPropertiesIteratorWithNull<EdgeID, types::UInt64>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Double>;
template class GetPropertiesIteratorWithNull<EdgeID, types::String>;
template class GetPropertiesIteratorWithNull<EdgeID, types::Bool>;

}

template <IteratedID ID, SupportedType T>
GetPropertiesChunkWriter<ID, T>::GetPropertiesChunkWriter(const GraphView& view,
                                                          PropertyTypeID propTypeID,
                                                          const ColumnIDs* inputIDs)
    : GetPropertiesIterator<ID, T>(view, propTypeID, inputIDs)
{
}

template <IteratedID ID, SupportedType T>
void GetPropertiesChunkWriter<ID, T>::fill(size_t maxCount) {
    _indices->clear();
    _output->clear();

    _output->reserve(this->_inputIDs->size());
    _indices->reserve(this->_inputIDs->size());

    while (this->isValid()) {
        const auto& currentProp = this->get();
        const size_t currIndex = std::distance(this->_inputIDs->cbegin(), this->_entityIt);

        _output->push_back(currentProp);
        _indices->push_back(currIndex);

        this->next();
    }
}

namespace db {

template class GetPropertiesChunkWriter<NodeID, types::Int64>;
template class GetPropertiesChunkWriter<NodeID, types::UInt64>;
template class GetPropertiesChunkWriter<NodeID, types::Double>;
template class GetPropertiesChunkWriter<NodeID, types::String>;
template class GetPropertiesChunkWriter<NodeID, types::Bool>;
template class GetPropertiesChunkWriter<EdgeID, types::Int64>;
template class GetPropertiesChunkWriter<EdgeID, types::UInt64>;
template class GetPropertiesChunkWriter<EdgeID, types::Double>;
template class GetPropertiesChunkWriter<EdgeID, types::String>;
template class GetPropertiesChunkWriter<EdgeID, types::Bool>;

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
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Int64>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Double>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::String>;
template class GetPropertiesWithNullChunkWriter<EdgeID, types::Bool>;

}
