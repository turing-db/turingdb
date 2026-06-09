#pragma once

#include <stddef.h>
#include <stdint.h>
#include <type_traits>

#include "DumpConfig.h"
#include "indexers/PropertyIndexer.h"

namespace db {

// The dumper bulk-writes PropertyRange object bytes, while the loader reads
// the two fields back individually. Pin the layout the on-disk format
// depends on.
static_assert(sizeof(PropertyRange) == 2 * sizeof(uint64_t), "Dump format writes PropertyRange object bytes as two packed uint64 values");
static_assert(std::is_trivially_copyable_v<PropertyRange>, "Dump format writes PropertyRange object bytes as two packed uint64 values");
static_assert(offsetof(PropertyRange, _offset) == 0, "Loaders read PropertyRange as offset then count");
static_assert(offsetof(PropertyRange, _count) == sizeof(uint64_t), "Loaders read PropertyRange as offset then count");

class PropertyIndexerDumperConstants {
public:
    // Page metadata
    static constexpr size_t PAGE_HEADER_STRIDE = sizeof(uint64_t);

    // Avail space in page
    static constexpr size_t PAGE_AVAIL = DumpConfig::PAGE_SIZE - PAGE_HEADER_STRIDE;

    // Strides
    static constexpr size_t LABELSET_INFO_STRIDE = 2 * sizeof(uint64_t);

    static size_t getInfoStride(const std::vector<PropertyRange>& info) {
        return sizeof(uint64_t)                    // Range count
             + info.size() * LABELSET_INFO_STRIDE; // Ranges
    }

    static size_t getPropTypeIndexerStride(const LabelSetPropertyIndexer& indexer) {
        size_t stride = sizeof(PropertyTypeID::Type) // Property type ID
                      + sizeof(uint64_t);            // LabelSet count

        for (const auto& [lsetID, info] : indexer) {
            stride += sizeof(LabelSetID::Type);
            stride += getInfoStride(info);
        }

        return stride;
    }
};

}
