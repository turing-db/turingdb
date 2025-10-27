#include "ScanNodePropertiesByLabelIterator.h"

#include "DataPart.h"
#include "IteratorUtils.h"
#include "iterators/TombstoneFilter.h"
#include "properties/PropertyManager.h"
#include "Panic.h"

namespace db {

template <SupportedType T>
ScanNodePropertiesByLabelIterator<T>::ScanNodePropertiesByLabelIterator(
    const GraphView& view,
    PropertyTypeID propTypeID,
    const LabelSetHandle& labelset)
    : Iterator(view),
      _propTypeID(propTypeID),
      _labelset(labelset)
{
    init();
}

template <SupportedType T>
ScanNodePropertiesByLabelIterator<T>::~ScanNodePropertiesByLabelIterator() = default;

template <SupportedType T>
void ScanNodePropertiesByLabelIterator<T>::init() {
    for (; _partIt.isNotEnd(); _partIt.next()) {
        const DataPart* part = _partIt.get();
        const PropertyManager& nodeProperties = part->nodeProperties();

        if (nodeProperties.hasPropertyType(_propTypeID)) {
            const auto& indexer = nodeProperties.getIndexer(_propTypeID);
            const TypedPropertyContainer<T>& props = nodeProperties.getContainer<T>(_propTypeID);
            _labelsetIt = indexer.matchIterate(_labelset);

            for (; _labelsetIt.isValid(); _labelsetIt.next()) {
                const auto& ranges = _labelsetIt.getValue();
                _rangeIt = ranges.begin();

                for (; _rangeIt != ranges.end(); _rangeIt++) {
                    const auto& range = *_rangeIt;

                    _props = props.getSpan(range._offset, range._count);
                    _propIt = _props.begin();
                    _currentIDIt = std::span {props.ids()}.begin() + range._offset;

                    if (!_props.empty()) {
                        return;
                    }
                }
            }
        }
    }
}

template <SupportedType T>
void ScanNodePropertiesByLabelIterator<T>::next() {
    _propIt++;
    _currentIDIt++;
    nextValid();
}

template <SupportedType T>
void ScanNodePropertiesByLabelIterator<T>::reset() {
    Iterator::reset();
    init();
}

template <SupportedType T>
void ScanNodePropertiesByLabelIterator<T>::nextValid() {
    while (_propIt == _props.end()) {
        _rangeIt++;
        while (_rangeIt == _labelsetIt.getValue().end()) {
            _labelsetIt++;

            while (!_labelsetIt.isValid()) {
                _partIt.next();

                if (!_partIt.isNotEnd()) {
                    return;
                }

                const DataPart* part = _partIt.get();
                const PropertyManager& nodeProperties = part->nodeProperties();

                if (nodeProperties.hasPropertyType(_propTypeID)) {
                    const auto& indexer = nodeProperties.getIndexer(_propTypeID);
                    _labelsetIt = indexer.matchIterate(_labelset);
                }
            }

            _rangeIt = _labelsetIt.getValue().begin();
        }

        const DataPart* part = _partIt.get();
        const PropertyManager& nodeProperties = part->nodeProperties();
        const TypedPropertyContainer<T>& props = nodeProperties.getContainer<T>(_propTypeID);
        const auto& range = *_rangeIt;
        _props = props.getSpan(range._offset, range._count);
        _propIt = _props.begin();
        _currentIDIt = std::span {props.ids()}.begin() + range._offset;
    }
}

template <SupportedType T>
ScanNodePropertiesByLabelChunkWriter<T>::ScanNodePropertiesByLabelChunkWriter() = default;

template <SupportedType T>
ScanNodePropertiesByLabelChunkWriter<T>::ScanNodePropertiesByLabelChunkWriter(
    const GraphView& view,
    PropertyTypeID propTypeID,
    const LabelSetHandle& labelset)
    : ScanNodePropertiesByLabelIterator<T>(view, propTypeID, labelset)
{
}

template <SupportedType T>
void ScanNodePropertiesByLabelChunkWriter<T>::filterTombstones() {
    // XXX: This should probably be an exception, as it will cause segfault if the
    // planner does not materialise the edge column
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
void ScanNodePropertiesByLabelChunkWriter<T>::fill(size_t maxCount) {
    size_t remainingToMax = maxCount;
    msgbioassert(_properties || _nodeIDs,
                 "ScanNodePropertiesByLabelChunkWriter must be initialized with a valid column");
    static constexpr auto bools = generateArray<NColumns, NCombinations>();
    static constexpr auto masks = generateBitmasks<NColumns, NCombinations>();

    const auto getPrevSize = [&]() {
        if (_properties) {
            return _properties->size();
        }
        if (_nodeIDs) {
            return _nodeIDs->size();
        }
        panic("At least one column must be set");
    };

    if (_properties) {
        _properties->clear();
    }
    if (_nodeIDs) {
        _nodeIDs->clear();
    }

    const auto fill = [&]<std::array<bool, NColumns> conditions>() {
        while (this->isValid() && remainingToMax > 0) {
            const size_t availInPart = std::distance(this->_propIt, this->_props.end());
            const size_t rangeSize = std::min(remainingToMax, availInPart);
            const size_t prevSize = getPrevSize();
            const size_t newSize = prevSize + rangeSize;

            if constexpr (conditions[0]) {
                this->_properties->resize(newSize);
            }
            if constexpr (conditions[1]) {
                this->_nodeIDs->resize(newSize);
            }
            remainingToMax -= rangeSize;

            for (size_t i = prevSize; i < newSize; i++) {
                if constexpr (conditions[0]) {
                    (*this->_properties)[i] = *this->_propIt;
                }
                if constexpr (conditions[1]) {
                    (*this->_nodeIDs)[i] = this->_currentIDIt->getValue();
                }
                ++this->_propIt;
                ++this->_currentIDIt++;
            }
            this->nextValid();
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

template class ScanNodePropertiesByLabelIterator<types::Int64>;
template class ScanNodePropertiesByLabelIterator<types::UInt64>;
template class ScanNodePropertiesByLabelIterator<types::Double>;
template class ScanNodePropertiesByLabelIterator<types::String>;
template class ScanNodePropertiesByLabelIterator<types::Bool>;

template class ScanNodePropertiesByLabelChunkWriter<types::Int64>;
template class ScanNodePropertiesByLabelChunkWriter<types::UInt64>;
template class ScanNodePropertiesByLabelChunkWriter<types::Double>;
template class ScanNodePropertiesByLabelChunkWriter<types::String>;
template class ScanNodePropertiesByLabelChunkWriter<types::Bool>;

}
