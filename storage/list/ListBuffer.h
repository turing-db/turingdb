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
               db::types::Bool::Primitive, db::types::Embedding::Primitive, db::ListView>;
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

    /**
     * @brief Reserves contiguous storage for a list of @param numElements elements whose
     * values occupy @param valueBytes bytes in total, and returns the (as-yet unfilled)
     * @ref ListView spanning them.
     *
     * For streaming decoders: reserve once up front, then call @ref appendElement for each
     * element as it arrives. Reserving guarantees the element bytes and the views stay put,
     * so the returned ListView is valid immediately and the appends only fill it in.
     */
    ListView reserveList(size_t numElements, size_t valueBytes);

    /// Appends one element into the space reserved by the preceding @ref reserveList,
    /// returning the view of the element just written.
    ListElementView appendElement(const ListItemVariant& element);

    /// Appends a pre-formed [tag][value] element of @param numBytes bytes, copied
    /// directly without materialising an intermediate value, returning the view of the
    /// element just written. Only valid for elements whose stored layout matches the
    /// source bytes (i.e. the fixed-width types).
    /// @warn As mentioned ListByteBuffer.h this MUST ONLY be called after appropriate
    /// buffer resizing as there are no bound checks
    ListElementView appendRawElement(const void* data, size_t numBytes);

    void clear();

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
