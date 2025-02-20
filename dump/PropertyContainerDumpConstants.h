#pragma once

#include <stddef.h>
#include <stdint.h>

#include "EntityID.h"
#include "types/SupportedType.h"
#include "DumpConfig.h"
#include "StringBucket.h"

namespace db {

template <SupportedType T>
class TrivialPropertyContainerDumpConstants {
public:
    // ID page metadata stride
    static constexpr size_t ID_HEADER_STRIDE = sizeof(uint64_t); // Prop count

    // Property value page metadata stride
    static constexpr size_t VALUE_HEADER_STRIDE = sizeof(uint64_t); // Prop count

    // Single id stride
    static constexpr size_t ID_STRIDE = sizeof(EntityID::Type);

    // Single property value stride
    static constexpr size_t VALUE_STRIDE = sizeof(typename T::Primitive);

    // Avail space in id page
    static constexpr size_t ID_PAGE_AVAIL = DumpConfig::PAGE_SIZE - ID_HEADER_STRIDE;

    // Avail space in property value page
    static constexpr size_t VALUE_PAGE_AVAIL = DumpConfig::PAGE_SIZE - VALUE_HEADER_STRIDE;

    // ID count per page
    static constexpr size_t ID_COUNT_PER_PAGE = ID_PAGE_AVAIL / ID_STRIDE;

    // Value count per page
    static constexpr size_t VALUE_COUNT_PER_PAGE = ID_PAGE_AVAIL / VALUE_STRIDE;
};

class StringPropertyContainerDumpConstants {
public:
    // ID page metadata stride
    static constexpr size_t ID_HEADER_STRIDE = sizeof(uint64_t); // Prop count

    // Buckets page metadata stride
    static constexpr size_t BUCKETS_HEADER_STRIDE = sizeof(uint64_t); // Bucket count

    // Limits page metadata stride
    static constexpr size_t LIMITS_HEADER_STRIDE = sizeof(uint64_t); // Limit block count

    // Single id stride
    static constexpr size_t ID_STRIDE = sizeof(EntityID::Type);

    // Single bucket stride
    static constexpr size_t BUCKET_STRIDE = StringBucket::BUCKET_SIZE;

    // Single string limit stride
    static constexpr size_t LIMIT_STRIDE = sizeof(StringBucket::StringLimits);

    // Single string limit block stride
    static constexpr size_t LIMIT_BLOCK_STRIDE = sizeof(uint64_t)  // Bucket index
                                               + sizeof(uint32_t); // String count

    // Avail space in id page
    static constexpr size_t ID_PAGE_AVAIL = DumpConfig::PAGE_SIZE - ID_HEADER_STRIDE;

    // Avail space in bucket page
    static constexpr size_t BUCKET_PAGE_AVAIL = DumpConfig::PAGE_SIZE - BUCKETS_HEADER_STRIDE;

    // Avail space in limit stride
    static constexpr size_t LIMIT_PAGE_AVAIL = DumpConfig::PAGE_SIZE - LIMITS_HEADER_STRIDE;

    // ID count per page
    static constexpr size_t ID_COUNT_PER_PAGE = ID_PAGE_AVAIL / ID_STRIDE;

    // BUCKET count per page
    static constexpr size_t BUCKET_COUNT_PER_PAGE = BUCKET_PAGE_AVAIL / BUCKET_STRIDE;
};

}

