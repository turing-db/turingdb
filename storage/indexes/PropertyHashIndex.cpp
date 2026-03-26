#include "PropertyHashIndex.h"

#include "DataPart.h"
#include "metadata/PropertyType.h"
#include "properties/PropertyManager.h"

#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "ID.h"

using namespace db;

template <SupportedType P, TypedInternalID I>
PropertyHashIndex<P, I>::PropertyHashIndex(std::string_view propertyName,
                                           PropertyTypeID propertyID)
    : _propName(propertyName),
    _propID(propertyID)
{
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::init(GraphView view) {
    for (const WeakArc<DataPart>& part : view.dataparts()) {
        const PropertyManager* propManPtr = nullptr;

        if constexpr (isNode) {
            propManPtr = &part->nodeProperties();
        } else {
            propManPtr = &part->edgeProperties();
        }
        bioassert(propManPtr, "Failed to get PropertyManager while initialising index.");

        const PropertyManager& propMan = *propManPtr;
        const TypedPropertyContainer<P>& container = propMan.getContainer<P>(_propID);

        for (const auto& [id, val] : container.zipped()) {
            IDContainer& assoc = _data[val];
            assoc.emplace_back(id.getValue());
        }
    }
}

template <SupportedType P, TypedInternalID I>
void PropertyHashIndex<P, I>::query(const Column* query, Column* result) {
}

namespace db {
template class PropertyHashIndex<types::Int64,  NodeID>;
template class PropertyHashIndex<types::UInt64, NodeID>;
template class PropertyHashIndex<types::Double, NodeID>;
template class PropertyHashIndex<types::String, NodeID>;
template class PropertyHashIndex<types::Bool,   NodeID>;

template class PropertyHashIndex<types::Int64,  EdgeID>;
template class PropertyHashIndex<types::UInt64, EdgeID>;
template class PropertyHashIndex<types::Double, EdgeID>;
template class PropertyHashIndex<types::String, EdgeID>;
template class PropertyHashIndex<types::Bool,   EdgeID>;
}
