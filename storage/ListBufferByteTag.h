#pragma once

#include <stdint.h>

#include "metadata/PropertyType.h"

namespace db {

/// @brief Tag used in @ref ListByteBuffer to store type information
enum class ListBufferTypeTag : uint8_t {
    Int = 0,
    UInt,
    Double,
    Bool,
    String,
    Embedding,

    INVALID,
};

/// Helpers to convert types to tags
template <typename T>
struct TypeToListBufferTag;

template <>
struct TypeToListBufferTag<types::Int64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Int;
};

template <>
struct TypeToListBufferTag<types::UInt64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::UInt;
};

template <>
struct TypeToListBufferTag<types::Double::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Double;
};

template <>
struct TypeToListBufferTag<types::Bool::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Bool;
};

template <>
struct TypeToListBufferTag<types::String::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::String;
};

}
