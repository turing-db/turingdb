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

using InternalTypeCodeType = uint8_t;

inline constexpr InternalTypeCodeType FirstInternalTypeCodeValue = __COUNTER__;

inline constexpr InternalTypeCodeType InvalidInternalTypeCode = std::numeric_limits<InternalTypeCodeType>::max();

template <typename T>
inline constexpr InternalTypeCodeType InternalTypeCodeValue = InvalidInternalTypeCode;

template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<size_t> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<EntityID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<NodeID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<EdgeID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<LabelSetID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<LabelSet> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<EdgeTypeID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<LabelID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<PropertyTypeID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::string> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<ValueType> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<ChangeID> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<ColumnVector<EntityID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<ColumnMask> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<PropertyType> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<PropertyNull> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<bool> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<types::Int64::Primitive> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<types::Double::Primitive> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<types::Bool::Primitive> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<types::String::Primitive> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<EntityID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<NodeID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<EdgeID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<LabelSetID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<LabelSet>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<EdgeTypeID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<LabelID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<PropertyTypeID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<types::UInt64::Primitive>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<types::Int64::Primitive>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<types::Double::Primitive>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<types::Bool::Primitive>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<types::String::Primitive>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<std::string>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<ValueType>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<std::optional<ChangeID>> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<NodeView> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<const CommitBuilder*> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<const Change*> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<Column*> = __COUNTER__ - FirstInternalTypeCodeValue;
template <>
inline constexpr InternalTypeCodeType InternalTypeCodeValue<ValueVariant> = __COUNTER__ - FirstInternalTypeCodeValue;

/// @brief The number of internal types
//
// This value increase later if we add more internal types
inline constexpr size_t InternalTypeCount = __COUNTER__ - FirstInternalTypeCodeValue;

/// @brief The number of bits required to represent the internal type codes
inline constexpr size_t InternalTypeBitCount = 8;

inline constexpr size_t InternalTypeMaxValue = (2 << (InternalTypeBitCount - 1)) - 1;

static_assert(InternalTypeCount < InternalTypeMaxValue);

}
