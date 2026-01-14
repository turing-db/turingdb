#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <optional>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"

namespace db {

template <typename>
class ColumnVector;

template <typename>
class ColumnConst;

class ColumnMask;
class NodeView;
class CommitBuilder;
class Change;

template <std::integral TType, size_t TCount>
class TemplateLabelSet;

class ColumnKind {
public:
    using ColumnKindCode = uint64_t;

    enum class BaseColumnKind : ColumnKindCode {
        VECTOR = 0,
        CONST,
        MASK,
        SET,

        _SIZE,
    };

    // Explicit enum for internal type kinds to avoid __COUNTER__ portability issues
    // between compilers (GCC vs Clang produce different values)
    enum class InternalTypeKind : ColumnKindCode {
        SizeT = 0,
        EntityID,
        NodeID,
        EdgeID,
        LabelSetID,
        LabelSet,
        EdgeTypeID,
        LabelID,
        PropertyTypeID,
        StdString,
        ValueType,
        ChangeID,
        ColumnVectorEntityID,
        ColumnMask,
        UInt64Primitive,
        Int64Primitive,
        DoublePrimitive,
        BoolPrimitive,
        StringPrimitive,
        OptEntityID,
        OptNodeID,
        OptEdgeID,
        OptChangeID,
        OptLabelID,
        OptLabelSetID,
        OptEdgeTypeID,
        OptPropertyTypeID,
        OptSizeT,
        OptValueType,
        OptStdString,
        OptUInt64Primitive,
        OptInt64Primitive,
        OptDoublePrimitive,
        OptBoolPrimitive,
        OptStringPrimitive,
        CustomBool,
        NodeView,
        CommitBuilderPtr,
        ChangePtr,

        _SIZE,
    };

    using LabelSet = TemplateLabelSet<uint64_t, 4>;

    inline static consteval ColumnKindCode getBaseColumnKindCount() {
        return (ColumnKindCode)BaseColumnKind::_SIZE;
    }

    template <typename T>
    inline static consteval ColumnKindCode getInternalTypeKind() {
        // size_t and types::UInt64::Primitive may be the same type on some platforms
        // (both are unsigned long on Linux x86_64). When they are the same, map size_t
        // to UInt64Primitive to avoid duplicate ColumnKind values.
        if constexpr (std::is_same_v<T, size_t>) {
            if constexpr (std::is_same_v<size_t, types::UInt64::Primitive>) {
                return (ColumnKindCode)InternalTypeKind::UInt64Primitive;
            } else {
                return (ColumnKindCode)InternalTypeKind::SizeT;
            }
        } else if constexpr (std::is_same_v<T, EntityID>) {
            return (ColumnKindCode)InternalTypeKind::EntityID;
        } else if constexpr (std::is_same_v<T, NodeID>) {
            return (ColumnKindCode)InternalTypeKind::NodeID;
        } else if constexpr (std::is_same_v<T, EdgeID>) {
            return (ColumnKindCode)InternalTypeKind::EdgeID;
        } else if constexpr (std::is_same_v<T, LabelSetID>) {
            return (ColumnKindCode)InternalTypeKind::LabelSetID;
        } else if constexpr (std::is_same_v<T, LabelSet>) {
            return (ColumnKindCode)InternalTypeKind::LabelSet;
        } else if constexpr (std::is_same_v<T, EdgeTypeID>) {
            return (ColumnKindCode)InternalTypeKind::EdgeTypeID;
        } else if constexpr (std::is_same_v<T, LabelID>) {
            return (ColumnKindCode)InternalTypeKind::LabelID;
        } else if constexpr (std::is_same_v<T, PropertyTypeID>) {
            return (ColumnKindCode)InternalTypeKind::PropertyTypeID;
        } else if constexpr (std::is_same_v<T, std::string>) {
            return (ColumnKindCode)InternalTypeKind::StdString;
        } else if constexpr (std::is_same_v<T, ValueType>) {
            return (ColumnKindCode)InternalTypeKind::ValueType;
        } else if constexpr (std::is_same_v<T, ChangeID>) {
            return (ColumnKindCode)InternalTypeKind::ChangeID;
        } else if constexpr (std::is_same_v<T, ColumnVector<EntityID>>) {
            return (ColumnKindCode)InternalTypeKind::ColumnVectorEntityID;
        } else if constexpr (std::is_same_v<T, ColumnMask>) {
            return (ColumnKindCode)InternalTypeKind::ColumnMask;
        } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
            return (ColumnKindCode)InternalTypeKind::UInt64Primitive;
        } else if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
            return (ColumnKindCode)InternalTypeKind::Int64Primitive;
        } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
            return (ColumnKindCode)InternalTypeKind::DoublePrimitive;
        } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
            return (ColumnKindCode)InternalTypeKind::BoolPrimitive;
        } else if constexpr (std::is_same_v<T, types::String::Primitive>) {
            return (ColumnKindCode)InternalTypeKind::StringPrimitive;
        } else if constexpr (std::is_same_v<T, std::optional<EntityID>>) {
            return (ColumnKindCode)InternalTypeKind::OptEntityID;
        } else if constexpr (std::is_same_v<T, std::optional<NodeID>>) {
            return (ColumnKindCode)InternalTypeKind::OptNodeID;
        } else if constexpr (std::is_same_v<T, std::optional<EdgeID>>) {
            return (ColumnKindCode)InternalTypeKind::OptEdgeID;
        } else if constexpr (std::is_same_v<T, std::optional<ChangeID>>) {
            return (ColumnKindCode)InternalTypeKind::OptChangeID;
        } else if constexpr (std::is_same_v<T, std::optional<LabelID>>) {
            return (ColumnKindCode)InternalTypeKind::OptLabelID;
        } else if constexpr (std::is_same_v<T, std::optional<LabelSetID>>) {
            return (ColumnKindCode)InternalTypeKind::OptLabelSetID;
        } else if constexpr (std::is_same_v<T, std::optional<EdgeTypeID>>) {
            return (ColumnKindCode)InternalTypeKind::OptEdgeTypeID;
        } else if constexpr (std::is_same_v<T, std::optional<PropertyTypeID>>) {
            return (ColumnKindCode)InternalTypeKind::OptPropertyTypeID;
        } else if constexpr (std::is_same_v<T, std::optional<size_t>>) {
            if constexpr (std::is_same_v<size_t, types::UInt64::Primitive>) {
                return (ColumnKindCode)InternalTypeKind::OptUInt64Primitive;
            } else {
                return (ColumnKindCode)InternalTypeKind::OptSizeT;
            }
        } else if constexpr (std::is_same_v<T, std::optional<ValueType>>) {
            return (ColumnKindCode)InternalTypeKind::OptValueType;
        } else if constexpr (std::is_same_v<T, std::optional<std::string>>) {
            return (ColumnKindCode)InternalTypeKind::OptStdString;
        } else if constexpr (std::is_same_v<T, std::optional<types::UInt64::Primitive>>) {
            return (ColumnKindCode)InternalTypeKind::OptUInt64Primitive;
        } else if constexpr (std::is_same_v<T, std::optional<types::Int64::Primitive>>) {
            return (ColumnKindCode)InternalTypeKind::OptInt64Primitive;
        } else if constexpr (std::is_same_v<T, std::optional<types::Double::Primitive>>) {
            return (ColumnKindCode)InternalTypeKind::OptDoublePrimitive;
        } else if constexpr (std::is_same_v<T, std::optional<types::Bool::Primitive>>) {
            return (ColumnKindCode)InternalTypeKind::OptBoolPrimitive;
        } else if constexpr (std::is_same_v<T, std::optional<types::String::Primitive>>) {
            return (ColumnKindCode)InternalTypeKind::OptStringPrimitive;
        } else if constexpr (std::is_same_v<T, CustomBool>) {
            return (ColumnKindCode)InternalTypeKind::CustomBool;
        } else if constexpr (std::is_same_v<T, NodeView>) {
            return (ColumnKindCode)InternalTypeKind::NodeView;
        } else if constexpr (std::is_same_v<T, const CommitBuilder*>) {
            return (ColumnKindCode)InternalTypeKind::CommitBuilderPtr;
        } else if constexpr (std::is_same_v<T, const Change*>) {
            return (ColumnKindCode)InternalTypeKind::ChangePtr;
        }

        return -1;
    }

    inline static consteval ColumnKindCode getInternalTypeKindCount() {
        return (ColumnKindCode)InternalTypeKind::_SIZE;
    }

    template <typename T>
    inline static consteval ColumnKindCode getColumnKind() {
        const ColumnKindCode baseColumnKind = T::BaseKind;
        const ColumnKindCode internalTypeColumnKind = getInternalTypeKind<typename T::ValueType>();
        const ColumnKindCode internalTypeKindCount = getInternalTypeKindCount();
        return baseColumnKind * internalTypeKindCount + internalTypeColumnKind;
    }

    inline static consteval ColumnKindCode getColumnKindCount() {
        return getBaseColumnKindCount() * getInternalTypeKindCount();
    }

    inline static consteval ColumnKindCode getBasePairColumnCount() {
        ColumnKindCode count = 2;
        for (size_t i = 1; i < getBaseColumnKindCount(); i++) {
            count *= 2;
        }
        return count;
    }
};

}
