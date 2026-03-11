#pragma once

#include <cstdint>

enum class AgingRingCacheState : uint8_t {
    LOADING = 0,
    RESIDENT,
    EVICTING,
};
