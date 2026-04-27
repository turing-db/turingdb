#pragma once

#include <stddef.h>
#include <utility>

#include "ListByteBuffer.h"
#include "ListElementViewBuffer.h"

#include "ListBufferTypeTag.h"

#include "ListView.h"

#include "metadata/PropertyType.h"

#include "TypeUtils.h"

namespace {
using ListableTypesImpl =
    std::tuple<db::types::Int64::Primitive, db::types::UInt64::Primitive,
               db::types::Double::Primitive, db::types::String::Primitive,
               db::types::Bool::Primitive, db::types::Embedding::Primitive>;
}

namespace db {

/**
 * @brief Container to store heterogeneous lists. Guarantees elements that are
 * inserted as a result of a single call to @ref insert are contiguous. References are
 * permamently stable.
 *
 * @detail Stores list elements (in @ref _elements) alongside a non-owning view for each
 * element (in @ref ListElementViewBuffer).
 *
 * @tparam N size {in bytes, in elements} of each chunk of the underlying
 * {@ref ListByteBuffer, @ref ListElementViewBuffer}.
 */
template <size_t N = 4096>
class ListBuffer {
public:
    using ListableTypes = ListableTypesImpl;
    using ListItemVariant = TypeUtils::tuple_to_variant_t<ListableTypes>;

    /**
     * @brief Given the provided @param elements, stores those elements in contiguous,
     * stable memory, and returns a stable @ref ListView into each stored element.
     */
    ListView insert(std::span<const ListItemVariant> elements);

private:
    /// Container of raw bytes for each element
    ListByteBuffer<N> _elements;
    /// Container of @ref ListElementView, for each element in @ref _elements
    ListElementViewBuffer<N> _views;
};

template <typename T>
concept Listable = TypeConcepts::InTuple<T, ListableTypesImpl>;

// Ensure we have a type tag for each listable type
static_assert(std::tuple_size_v<ListableTypesImpl>
              == std::to_underlying(ListBufferTypeTag::INVALID));
}
