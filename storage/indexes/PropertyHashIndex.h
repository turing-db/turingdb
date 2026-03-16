#pragma once

#include <type_traits>
#include <unordered_map>

#include "Index.h"

#include "ID.h"
#include "metadata/SupportedType.h"

namespace db {

template <typename T>
class ColumnVector;

template <typename T>
class ColumnConst;

template <SupportedType P, TypedInternalID I>
class PropertyHashIndex final : Index {
public:
    using IDContainer = ColumnVector<I>;
    using PropertyPrimitive = P::Primitive;
    using PropertyValueHash = std::unordered_map<PropertyPrimitive, IDContainer>;

    PropertyHashIndex(std::string_view propertyName, PropertyTypeID propertyID);

    void init(GraphView view) final;

    const Column* query(const Column* col) final;
private:
    std::string _propName;
    PropertyTypeID _propID;
    PropertyValueHash _data;

    IDContainer _empty {};
    static constexpr bool isNode = std::is_same_v<I, NodeID>;
};

}
