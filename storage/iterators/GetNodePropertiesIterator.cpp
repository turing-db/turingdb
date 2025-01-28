#include "GetNodePropertiesIterator.h"

#include "DataPart.h"
#include "properties/PropertyManager.h"

using namespace db;

template <SupportedType T>
GetNodePropertiesIterator<T>::GetNodePropertiesIterator(const GraphView& view,
                                                        PropertyTypeID propTypeID,
                                                        const ColumnIDs* inputNodeIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputNodeIDs(inputNodeIDs)
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
            _nodeIt = _inputNodeIDs->cbegin();

            for (; _nodeIt != _inputNodeIDs->cend(); _nodeIt++) {
                _prop = properties.tryGet<T>(propTypeID, *_nodeIt);

                if (_prop) {
                    return;
                }
            }
        }
    }
}

template <SupportedType T>
void GetNodePropertiesIterator<T>::next() {
    // Remmember that we loop on input node IDs for each DataPart,
    // so that we gather the info on a given node from all DataParts

    // Reset the _prop pointer
    _prop = nullptr;

    while (!_prop) {
        _nodeIt++;

        // if no more inputNodeIDs, we can find the next valid DataPart
        // and start over the iteration on inputNodeIDs
        while (_nodeIt == _inputNodeIDs->end()) {
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
            _nodeIt = _inputNodeIDs->cbegin();
        }

        // From here, _partIt and _nodeIt are both valid
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = part->nodeProperties();

        _prop = properties.tryGet<T>(_propTypeID, *_nodeIt);
        // If _prop is nullptr, the current nodeID does not have this prop,
        // -> start over the algorithm
        // Else, the outer while condition is broken,
        // -> we found the next valid prop
    }
}

namespace db {

template class GetNodePropertiesIterator<types::Int64>;
template class GetNodePropertiesIterator<types::UInt64>;
template class GetNodePropertiesIterator<types::Double>;
template class GetNodePropertiesIterator<types::String>;
template class GetNodePropertiesIterator<types::Bool>;

}

// GetNodePropertiesIteratorWithNull
template <SupportedType T>
GetNodePropertiesIteratorWithNull<T>::GetNodePropertiesIteratorWithNull(const GraphView& view,
                                                                        PropertyTypeID propTypeID,
                                                                        const ColumnIDs* inputNodeIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputNodeIDs(inputNodeIDs)
{
    init();
}

template <SupportedType T>
void GetNodePropertiesIteratorWithNull<T>::init() {
    if (_inputNodeIDs->empty()) {
        return;
    }

    _nodeIt = _inputNodeIDs->cbegin();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = part->nodeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, *_nodeIt);
            if (_prop) {
                return;
            }
        }
    }
}

template <SupportedType T>
void GetNodePropertiesIteratorWithNull<T>::reset() {
    Iterator::reset();
    _nodeIt = _inputNodeIDs->cbegin();
    init();
}

template <SupportedType T>
void GetNodePropertiesIteratorWithNull<T>::next() {
    // Reset the _prop pointer
    _prop = nullptr;

    _nodeIt++;
    if (_nodeIt == _inputNodeIDs->end()) {
        return;
    }

    Iterator::reset();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = part->nodeProperties();

        if (properties.hasPropertyType(_propTypeID)) {
            _prop = properties.tryGet<T>(_propTypeID, *_nodeIt);
            if (_prop) {
                return;
            }
        }
    }
}

namespace db {

template class GetNodePropertiesIteratorWithNull<types::Int64>;
template class GetNodePropertiesIteratorWithNull<types::UInt64>;
template class GetNodePropertiesIteratorWithNull<types::Double>;
template class GetNodePropertiesIteratorWithNull<types::String>;
template class GetNodePropertiesIteratorWithNull<types::Bool>;

}

// GetNodePropertiesWithNullChunkWriter
template <SupportedType T>
GetNodePropertiesWithNullChunkWriter<T>::GetNodePropertiesWithNullChunkWriter(const GraphView& view,
                                                                              PropertyTypeID propTypeID,
                                                                              const ColumnIDs* inputNodeIDs)
    : GetNodePropertiesIteratorWithNull<T>(view, propTypeID, inputNodeIDs)
{
}

template <SupportedType T>
void GetNodePropertiesWithNullChunkWriter<T>::fill(size_t maxCount) {
    auto& output = *_output;
    const size_t availableSize = std::distance(this->_nodeIt, this->_inputNodeIDs->end());
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

template class GetNodePropertiesWithNullChunkWriter<types::Int64>;
template class GetNodePropertiesWithNullChunkWriter<types::UInt64>;
template class GetNodePropertiesWithNullChunkWriter<types::Double>;
template class GetNodePropertiesWithNullChunkWriter<types::String>;
template class GetNodePropertiesWithNullChunkWriter<types::Bool>;

}
