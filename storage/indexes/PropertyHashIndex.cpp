#include "PropertyHashIndex.h"

#include <range/v3/view/reverse.hpp>

#include "datapart/DataPart.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "ID.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

template <SupportedType P, TypedInternalID I>
PropertyHashIndex<P, I>::PropertyHashIndex(std::string_view name,
                                           PropertyTypeID propertyID)
    : Index(name),
    _propID(propertyID)
{
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::init(GraphView view) {
    // Store which IDs we have indexed for this property, so that we do not store both the
    // pre and post SET values, and instead only store the most recent value, since we
    // iterate backwards.
    std::unordered_set<I> indexedIDs;
    for (const WeakArc<DataPart>& part : rv::reverse(view.dataparts())) {
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

        for (const auto& [entID, val] : container->zipped()) {
            const I id {entID.getValue()};
            if (indexedIDs.contains(id)) {
                continue;
            }
            indexedIDs.emplace(id);

            IDContainer& assoc = _hashTable[val];
            assoc.emplace_back(id);
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

        result->resize(sz);
        for (size_t i = 0; i < sz; i++) {
            result->operator[](i) = matches[i];
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

    result->resize(sz);
    for (size_t i = 0; i < sz; i++) {
        result->operator[](i) = matches[i];
    }
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::boundedQuery(const Column* input,
                                           Column* result,
                                           ColumnIndices* indices,
                                           Index::QueryState& state,
                                           size_t limit) const {
    auto* output = dynamic_cast<ColumnVector<I>*>(result);
    bioassert(output, "Invalid output column to property index query.");

    const auto* vecInput = dynamic_cast<const ColumnVector<PropertyPrimitive>*>(input);

    if (vecInput) {
        boundedQuery(vecInput, output, indices, state, limit);
        return;
    }

    bioassert(false, "Invalid input column to PropertyHashIndex query.");
}


template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::boundedQuery(const ColumnVector<PropertyPrimitive>* input,
                                           ColumnVector<I>* result,
                                           ColumnIndices* indices,
                                           Index::QueryState& state,
                                           size_t limit) const {
    result->clear();
    indices->clear();

    size_t remaining = limit;

    // Start the query where it last left off: stored in the @param state.
    size_t& keyOffset = state._keyIndex;
    size_t& valueOffset = state._valueIndex;

    bioassert(keyOffset < input->size(), "Key offset exceeded input size.");

    for (size_t i = keyOffset; i < input->size(); i++) {
        const PropertyPrimitive& currentKey = input->operator[](i);
        const auto findIt = _hashTable.find(currentKey);
        const bool contains = findIt != end(_hashTable);

        if (!contains) {
            keyOffset = i + 1; // Start from next key...
            valueOffset = 0;   // ... at the beginning
            continue;
        }

        const IDContainer& matches = findIt->second;
        const size_t sz = matches.size();

        bioassert(valueOffset < sz, "Value offset exceeded match size.");

        for (size_t j = valueOffset; j < sz; j++) {
            if (remaining == 0) {
                // We have reached the bound limit for this call: return
                return;
            }
            const I id = matches[j];
            result->push_back(id);
            indices->push_back(keyOffset);

            valueOffset++;
            remaining--;
        }

        keyOffset = i + 1; // Start at next key...
        valueOffset = 0;   // ... at the beginning
    }
    state._finished = true;
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
