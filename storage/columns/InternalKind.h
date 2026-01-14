#pragma once

#include <concepts>
#include <cstdint>
#include <cstddef>
#include <string>

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

namespace db {

// Forward declarations

template <std::integral TType, size_t TCount>
class TemplateLabelSet;

template <std::integral TType, size_t TCount>
class TemplateLabelSetHandle;

using LabelSet = TemplateLabelSet<uint64_t, 4>;

template <typename T>
class ColumnVector;

class ColumnMask;
class NodeView;
class CommitBuilder;
class Change;
class Column;
class ValueVariant;

// Implementation

#define INSTANTIATE_INTERNAL_TYPE_CODE(T)    \
    static consteval Code codeImpl(tag<T>) { \
        return __COUNTER__ - FirstValue;    \
    }

class InternalKind {
public:
    using Code = uint8_t;

    /// @brief Returns the code for the given type
    template <typename T>
    static consteval Code code() {
        constexpr auto code = codeImpl(tag<T> {});
        static_assert(code != Invalid, "Internal type was not registered as a valid internal type");
        return code;
    }

    /// @brief Value indicating an invalid internal type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the internal type codes
    static constexpr size_t BitCount = 8;

    /// @brief The maximum value of the internal type codes
    static constexpr size_t MaxValue = (2 << (BitCount - 1)) - 1;

private:
    static constexpr Code FirstValue = __COUNTER__;

    template <typename T>
    struct tag {};

    INSTANTIATE_INTERNAL_TYPE_CODE(size_t);
    INSTANTIATE_INTERNAL_TYPE_CODE(EntityID);
    INSTANTIATE_INTERNAL_TYPE_CODE(NodeID);
    INSTANTIATE_INTERNAL_TYPE_CODE(EdgeID);
    INSTANTIATE_INTERNAL_TYPE_CODE(LabelSetID);
    INSTANTIATE_INTERNAL_TYPE_CODE(LabelSet);
    INSTANTIATE_INTERNAL_TYPE_CODE(EdgeTypeID);
    INSTANTIATE_INTERNAL_TYPE_CODE(LabelID);
    INSTANTIATE_INTERNAL_TYPE_CODE(PropertyTypeID);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::string);
    INSTANTIATE_INTERNAL_TYPE_CODE(ValueType);
    INSTANTIATE_INTERNAL_TYPE_CODE(ChangeID);
    INSTANTIATE_INTERNAL_TYPE_CODE(ColumnVector<EntityID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(ColumnMask);
    INSTANTIATE_INTERNAL_TYPE_CODE(PropertyType);
    INSTANTIATE_INTERNAL_TYPE_CODE(PropertyNull);
    INSTANTIATE_INTERNAL_TYPE_CODE(bool);
    INSTANTIATE_INTERNAL_TYPE_CODE(types::Int64::Primitive);
    INSTANTIATE_INTERNAL_TYPE_CODE(types::Double::Primitive);
    INSTANTIATE_INTERNAL_TYPE_CODE(types::Bool::Primitive);
    INSTANTIATE_INTERNAL_TYPE_CODE(types::String::Primitive);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<EntityID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<NodeID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<EdgeID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<LabelSetID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<LabelSet>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<EdgeTypeID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<LabelID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<PropertyTypeID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<types::UInt64::Primitive>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<types::Int64::Primitive>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<types::Double::Primitive>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<types::Bool::Primitive>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<types::String::Primitive>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<std::string>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<ValueType>);
    INSTANTIATE_INTERNAL_TYPE_CODE(std::optional<ChangeID>);
    INSTANTIATE_INTERNAL_TYPE_CODE(NodeView);
    INSTANTIATE_INTERNAL_TYPE_CODE(const CommitBuilder*);
    INSTANTIATE_INTERNAL_TYPE_CODE(const Change*);
    INSTANTIATE_INTERNAL_TYPE_CODE(Column*);
    INSTANTIATE_INTERNAL_TYPE_CODE(ValueVariant);

public:
    /// @brief Number of internal type codes
    static constexpr size_t Count = __COUNTER__ - FirstValue;

    static_assert(Count < MaxValue);
};

}
