#include "PropertyHashIndex.h"

#include "DataPart.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "ID.h"

using namespace db;

template <SupportedType P, TypedInternalID I>
PropertyHashIndex<P, I>::PropertyHashIndex(std::string_view name,
                                           PropertyTypeID propertyID)
    : Index(name),
    _propID(propertyID)
{
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::init(GraphView view) {
    for (const WeakArc<DataPart>& part : view.dataparts()) {
        const PropertyManager* propManPtr = nullptr;

        if constexpr (_isNode) {
            propManPtr = &part->nodeProperties();
        } else {
            propManPtr = &part->edgeProperties();
        }
        bioassert(propManPtr, "Failed to get PropertyManager while initialising index.");

        const PropertyManager& propMan = *propManPtr;
        const TypedPropertyContainer<P>* container = propMan.tryGetContainer<P>(_propID);

        if (!container) { // This DP doesn't have this property
            continue;
        }

        for (const auto& [id, val] : container->zipped()) {
            IDContainer& assoc = _hashTable[val];
            assoc.emplace_back(id.getValue());
        }
    }

    _initialised = true;
}

template <SupportedType P, TypedInternalID I>
size_t PropertyHashIndex<P, I>::size() const {
    size_t size {0};
    for (auto& [_, v] : _hashTable) {
        const PropertyHashIndex::IDContainer& col = v;
        size += col.size();
    }
    return size;
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::query(const Column* input, Column* result) const {
    auto* output = dynamic_cast<ColumnVector<I>*>(result);
    bioassert(output, "Invalid output column to property index query.");

    auto* vecInput = dynamic_cast<const ColumnVector<PropertyPrimitive>*>(input);

    if (vecInput) {
        query(vecInput, output);
        return;
    }

    auto* constInput = dynamic_cast<const ColumnConst<PropertyPrimitive>*>(input);

    if (constInput) {
        query(constInput, output);
        return;
    }

    bioassert(false, "Invalid input column to PropertyHashIndex query.");
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::query(const ColumnVector<PropertyPrimitive>* input,
                                    ColumnVector<I>* result) const {
    result->clear();

    for (const PropertyPrimitive propValue : *input) {
        const auto findIt = _hashTable.find(propValue);
        const bool contains = findIt != _hashTable.end();
        if (!contains) {
            continue;
        }

        const IDContainer& matches = findIt->second;
        const size_t sz = matches.size();

        for (size_t i = 0; i < sz; i++) {
            result->push_back(matches[i]);
        }
    }
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::query(const ColumnConst<PropertyPrimitive>* input,
                                    ColumnVector<I>* result) const {
    result->clear();

    const PropertyPrimitive propValue = input->getRaw();

    const auto findIt = _hashTable.find(propValue);
    const bool contains = findIt != _hashTable.end();
    if (!contains) {
        return;
    }

    const IDContainer& matches = findIt->second;
    const size_t sz = matches.size();

    for (size_t i = 0; i < sz; i++) {
        result->push_back(matches[i]);
    }
}

namespace db {
template class PropertyHashIndex<types::Int64, NodeID>;
template class PropertyHashIndex<types::UInt64, NodeID>;
template class PropertyHashIndex<types::Double, NodeID>;
template class PropertyHashIndex<types::String, NodeID>;
template class PropertyHashIndex<types::Bool, NodeID>;
template class PropertyHashIndex<types::Embedding, NodeID>;

template class PropertyHashIndex<types::Int64, EdgeID>;
template class PropertyHashIndex<types::UInt64, EdgeID>;
template class PropertyHashIndex<types::Double, EdgeID>;
template class PropertyHashIndex<types::String, EdgeID>;
template class PropertyHashIndex<types::Bool, EdgeID>;
template class PropertyHashIndex<types::Embedding, EdgeID>;
}
