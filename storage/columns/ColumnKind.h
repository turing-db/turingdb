#pragma once

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <optional>

#include "ID.h"
#include "metadata/PropertyType.h"

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

    using LabelSet = TemplateLabelSet<uint64_t, 4>;

    inline static consteval ColumnKindCode getBaseColumnKindCount() {
        return (ColumnKindCode)BaseColumnKind::_SIZE;
    }

    template <typename T>
    inline static consteval ColumnKindCode getInternalTypeKind() {
        const ColumnKindCode minKind = __COUNTER__ + 1;
        static_assert(minKind == 1);

        if constexpr (std::is_same_v<T, size_t>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, EntityID>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, NodeID>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, EdgeID>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, LabelSetID>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, LabelSet>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::string>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, ColumnVector<EntityID>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, ColumnMask>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, types::UInt64::Primitive>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, types::Int64::Primitive>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, types::Double::Primitive>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, types::Bool::Primitive>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, types::String::Primitive>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::optional<types::UInt64::Primitive>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::optional<types::Int64::Primitive>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::optional<types::Double::Primitive>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::optional<types::Bool::Primitive>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, std::optional<types::String::Primitive>>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, CustomBool>) {
            return  __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, NodeView>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, const CommitBuilder*>) {
            return __COUNTER__ - minKind;
        } else if constexpr (std::is_same_v<T, const Change*>) {
            return __COUNTER__ - minKind;
        }

        return -1;
    }

    inline static consteval ColumnKindCode getInternalTypeKindCount() {
        return __COUNTER__ - 1;
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
