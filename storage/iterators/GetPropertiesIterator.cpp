#include "GetPropertiesIterator.h"

#include "DataPart.h"
#include "properties/PropertyManager.h"

using namespace db;

template <typename IteratorTag, SupportedType T>
GetPropertiesIterator<IteratorTag, T>::GetPropertiesIterator(const GraphView& view,
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

template <typename IteratorTag, SupportedType T>
void GetPropertiesIterator<IteratorTag, T>::next() {
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

        const PropertyManager& properties = (std::is_same_v<IteratorTag, GetNodePropertiesIteratorTag>)
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

template class GetPropertiesIterator<GetNodePropertiesIteratorTag, types::Int64>;
template class GetPropertiesIterator<GetNodePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesIterator<GetNodePropertiesIteratorTag, types::Double>;
template class GetPropertiesIterator<GetNodePropertiesIteratorTag, types::String>;
template class GetPropertiesIterator<GetNodePropertiesIteratorTag, types::Bool>;
template class GetPropertiesIterator<GetEdgePropertiesIteratorTag, types::Int64>;
template class GetPropertiesIterator<GetEdgePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesIterator<GetEdgePropertiesIteratorTag, types::Double>;
template class GetPropertiesIterator<GetEdgePropertiesIteratorTag, types::String>;
template class GetPropertiesIterator<GetEdgePropertiesIteratorTag, types::Bool>;

}

template <typename IteratorTag, SupportedType T>
GetPropertiesIteratorWithNull<IteratorTag, T>::GetPropertiesIteratorWithNull(const GraphView& view,
                                                                            PropertyTypeID propTypeID,
                                                                            const ColumnIDs* inputEntityIDs)
    : Iterator(view),
      _propTypeID(propTypeID),
      _inputEntityIDs(inputEntityIDs)
{
    init();
}

template <typename IteratorTag, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorTag, T>::init() {
    if (_inputEntityIDs->empty()) {
        return;
    }

    _entityIt = _inputEntityIDs->cbegin();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = (std::is_same_v<IteratorTag, GetNodePropertiesIteratorTag>)
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

template <typename IteratorTag, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorTag, T>::reset() {
    Iterator::reset();
    _entityIt = _inputEntityIDs->cbegin();
    init();
}

template <typename IteratorTag, SupportedType T>
void GetPropertiesIteratorWithNull<IteratorTag, T>::next() {
    // Reset the _prop pointer
    _prop = nullptr;

    _entityIt++;
    if (_entityIt == _inputEntityIDs->end()) {
        return;
    }

    Iterator::reset();
    for (; _partIt.isValid(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& properties = (std::is_same_v<IteratorTag, GetNodePropertiesIteratorTag>)
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

template class GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, types::Int64>;
template class GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, types::Double>;
template class GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, types::String>;
template class GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, types::Bool>;
template class GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, types::Int64>;
template class GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, types::Double>;
template class GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, types::String>;
template class GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, types::Bool>;

}

template <typename IteratorTag, SupportedType T>
GetPropertiesWithNullChunkWriter<IteratorTag, T>::GetPropertiesWithNullChunkWriter(const GraphView& view,
                                                                                   PropertyTypeID propTypeID,
                                                                                   const ColumnIDs* inputEntityIDs)
    : GetPropertiesIteratorWithNull<IteratorTag, T>(view, propTypeID, inputEntityIDs)
{
}

template <typename IteratorTag, SupportedType T>
void GetPropertiesWithNullChunkWriter<IteratorTag, T>::fill(size_t maxCount) {
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

template class GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, types::Int64>;
template class GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, types::Double>;
template class GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, types::String>;
template class GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, types::Bool>;
template class GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, types::Int64>;
template class GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, types::UInt64>;
template class GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, types::Double>;
template class GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, types::String>;
template class GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, types::Bool>;

}
