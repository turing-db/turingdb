#pragma once

#include "metadata/PropertyType.h"

#include "ListBuffer.h"

namespace db {

template <typename T>
struct TypeToListBufferTag;

template <>
struct TypeToListBufferTag<types::Int64::Primitive> {
    static constexpr ListBuffer::ListBufferTag Tag = ListBuffer::ListBufferTag::Int;
};

template <>
struct TypeToListBufferTag<types::Double::Primitive> {
    static constexpr ListBuffer::ListBufferTag Tag = ListBuffer::ListBufferTag::Double;
};

}
