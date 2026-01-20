#pragma once

#include <concepts>
#include <stdint.h>
#include <stddef.h>
#include <string>

#include "columns/KindTypes.h"
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

// Implementation

class InternalKind {
public:
    using Types = KindTypes<
        signed char,
        unsigned char,
        short int,
        unsigned short int,
        int,
        unsigned int,
        long int,
        unsigned long int,
        long long int,
        unsigned long long int,
        EntityID,
        NodeID,
        EdgeID,
        LabelSetID,
        LabelSet,
        EdgeTypeID,
        LabelID,
        PropertyTypeID,
        std::string,
        ValueType,
        ChangeID,
        ColumnVector<EntityID>,
        ColumnMask,
        PropertyType,
        PropertyNull,
        bool,
        types::Double::Primitive,
        types::Bool::Primitive,
        types::String::Primitive,
        std::optional<EntityID>,
        std::optional<NodeID>,
        std::optional<EdgeID>,
        std::optional<LabelSetID>,
        std::optional<LabelSet>,
        std::optional<EdgeTypeID>,
        std::optional<LabelID>,
        std::optional<PropertyTypeID>,
        std::optional<types::UInt64::Primitive>,
        std::optional<types::Int64::Primitive>,
        std::optional<types::Double::Primitive>,
        std::optional<types::Bool::Primitive>,
        std::optional<types::String::Primitive>,
        std::optional<std::string>,
        std::optional<ValueType>,
        std::optional<ChangeID>,
        NodeView,
        const CommitBuilder*,
        const Change*,
        Column*>;

    using Code = uint8_t;

    /// @brief Returns the code for the given type
    template <typename T>
    static consteval Code code() {
        constexpr auto code = (Code)Types::indexOf<T>();
        static_assert(code != Invalid,
                      "Internal type was not registered as a valid internal type");
        return code;
    }

    /// @brief Value indicating an invalid internal type code
    static constexpr Code Invalid = std::numeric_limits<Code>::max();

    /// @brief The number of bits required to represent the internal type codes
    static constexpr size_t BitCount = sizeof(Code) * 8;

    /// @brief The maximum value of the internal type codes
    static constexpr Code MaxValue = (2ull << (BitCount - 1ull)) - 2ull;

    /// @brief The number of internal type codes
    static constexpr size_t Count = Types::count();

    static_assert(MaxValue != Invalid);
};

}
