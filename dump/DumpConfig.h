#pragma once

#include <cstdint>

#include "BuildInfo.h"
#include "PageSizeConfig.h"

namespace db {

class DumpConfig {
public:
    static constexpr uint32_t ONE_BAD_CAFE = 0x1BADCAFE;
    static constexpr uint64_t VERSION = HEAD_COMMIT_TIMESTAMP;
    static constexpr uint64_t UP_TO_DATE_VERSION = 1744620073;
    static constexpr uint64_t PAGE_SIZE = fs::DEFAULT_PAGE_SIZE;

    static constexpr size_t SIZEOF_ONE_BAD_CAFE = sizeof(decltype(ONE_BAD_CAFE));
    static constexpr size_t SIZEOF_VERSION = sizeof(decltype(VERSION));
    static constexpr size_t SIZEOF_UP_TO_DATE_VERSION = sizeof(decltype(UP_TO_DATE_VERSION));
    static constexpr size_t SIZEOF_PAGE_SIZE = sizeof(decltype(PAGE_SIZE));

    static constexpr size_t FILE_HEADER_STRIDE = SIZEOF_ONE_BAD_CAFE + SIZEOF_VERSION;
};

}
