#pragma once

#include <stddef.h>
#include <stdint.h>

#include "DumpConfig.h"
#include "indexers/PropertyIndexer.h"

namespace db {

class PropertyIndexerDumperConstants {
public:
    // Page metadata
    static constexpr size_t PAGE_HEADER_STRIDE = sizeof(uint64_t);

    // Avail space in page
    static constexpr size_t PAGE_AVAIL = DumpConfig::PAGE_SIZE - PAGE_HEADER_STRIDE;

    // Strides
    static constexpr size_t LABELSET_INFO_STRIDE = sizeof(EntityID::Type)
                                                 + 2 * sizeof(uint64_t);

    static size_t getInfoStride(const LabelSetInfo& info) {
        return sizeof(bool)                                // Bindary searched
             + sizeof(uint64_t)                            // Range count
             + info._ranges.size() * LABELSET_INFO_STRIDE; // Ranges
    }

    static size_t getPropTypeIndexerStride(const LabelSetIndexer<LabelSetInfo>& indexer) {
        size_t stride = sizeof(PropertyTypeID::Type) // Property type ID
                      + sizeof(uint64_t); // LabelSet count

        for (const auto& [lsetID, info] : indexer) {
            stride += sizeof(LabelSetID::Type);
            stride += getInfoStride(info);
        }

        return stride;
    }
};

}

