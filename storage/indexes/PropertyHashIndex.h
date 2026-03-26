#pragma once

#include <type_traits>
#include <unordered_map>

#include "Index.h"

#include "columns/ColumnVector.h"

#include "ID.h"
#include "metadata/SupportedType.h"

namespace db {

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

template <SupportedType P, TypedInternalID I>
class PropertyHashIndex final : public Index {
public:
    using IDContainer = ColumnVector<I>;
    using PropertyPrimitive = P::Primitive;
    using PropertyValueHash = std::unordered_map<PropertyPrimitive, IDContainer>;

    explicit PropertyHashIndex(PropertyTypeID propertyID);

    void init(GraphView view) final;

    void query(const Column* query, Column* result) final;

private:
    PropertyTypeID _propID;
    PropertyValueHash _data;

    IDContainer _empty {};
    static constexpr bool isNode = std::is_same_v<I, NodeID>;
};

}
