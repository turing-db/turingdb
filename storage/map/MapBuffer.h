#pragma once

#include <span>
#include <stddef.h>

#include "MapByteBuffer.h"
#include "MapEntryViewBuffer.h"
#include "MapBufferTypeTag.h"
#include "MapView.h"

#include "list/ListView.h"
#include "metadata/PropertyType.h"
#include "TypeUtils.h"

namespace {
using MapableTypesImpl =
    std::tuple<db::types::Int64::Primitive, db::types::UInt64::Primitive,
               db::types::Double::Primitive, db::types::String::Primitive,
               db::types::Bool::Primitive, db::types::Embedding::Primitive,
               db::ListView, db::MapView>;
}

namespace db {

/**
 * @brief Container to store heterogeneous maps (string keys, typed values). Guarantees
 * entries that are inserted as a result of a single call to @ref insert are contiguous.
 * References are permanently stable.
 *
 * @detail Stores map entries (in @ref _entries) alongside a non-owning view for each
 * entry (in @ref MapEntryViewBuffer).
 *
 * @tparam N size {in bytes, in entries} of each chunk of the underlying
 * {@ref MapByteBuffer, @ref MapEntryViewBuffer}.
 */
template <size_t N = 4096>
class MapBuffer {
public:
    using MapableTypes = MapableTypesImpl;
    using MapItemVariant = TypeUtils::tuple_to_variant_t<MapableTypes>;

    struct MapKeyValuePair {
        std::string_view key;
        MapItemVariant value;
    };

    /**
     * @brief Given the provided @param entries, stores those key-value pairs in
     * contiguous, stable memory, and returns a stable @ref MapView into each stored
     * entry.
     */
    MapView insert(std::span<const MapKeyValuePair> entries);

    void clear();

private:
    /// Container of raw bytes for each entry
    MapByteBuffer<N> _entries;
    /// Container of @ref MapEntryView, for each entry in @ref _entries
    MapEntryViewBuffer<N> _views;
};

template <typename T>
concept Mappable = TypeConcepts::InTuple<T, MapableTypesImpl>;

// Ensure we have a type tag for each mappable type
static_assert(std::tuple_size_v<MapableTypesImpl>
              == std::to_underlying(MapBufferTypeTag::INVALID));

}
