#pragma once

#include <span>
#include <utility>
#include <stddef.h>

#include "MapByteBuffer.h"
#include "MapEntryViewBuffer.h"
#include "MapBufferTypeTag.h"
#include "MapView.h"
#include "MapWriteCursor.h"

#include "list/ListView.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "TypeUtils.h"

namespace {
using MappableTypesImpl =
    std::tuple<db::types::Int64::Primitive, db::types::UInt64::Primitive,
               db::types::Double::Primitive, db::types::Bool::Primitive,
               db::types::String::Primitive, db::types::Embedding::Primitive,
               db::ListView, db::MapView, db::PropertyNull>;
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
    using MappableTypes = MappableTypesImpl;
    using MapItemVariant = TypeUtils::tuple_to_variant_t<MappableTypes>;

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

    /**
     * @brief Reserves contiguous storage for a map of @param numEntries entries whose values
     * occupy @param valueBytes bytes in total, commits it, and returns a @ref MapWriteCursor
     * over it.
     *
     * For streaming decoders: reserve once up front, then fill the region key by key and value
     * by value as they arrive. Reserving guarantees the entry bytes and the views stay put, so
     * the cursor's @ref MapView is valid immediately and the raw writes only fill it in.
     *
     * @param valueBytes counts only the stored value objects; a key costs a @ref std::string_view
     * however long its characters are, so the per-entry key and tag overhead is added here.
     */
    MapWriteCursor reserveMap(size_t numEntries, size_t valueBytes);

    void clear();

private:
    /// Container of raw bytes for each entry
    MapByteBuffer<N> _entries;
    /// Container of @ref MapEntryView, for each entry in @ref _entries
    MapEntryViewBuffer<N> _views;
};

// Ensure we have a type tag for each mappable type
static_assert(std::tuple_size_v<MappableTypesImpl>
              == std::to_underlying(MapBufferTypeTag::INVALID));

}
