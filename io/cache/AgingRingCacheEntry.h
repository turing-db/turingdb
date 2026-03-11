#pragma once

#include <list>
#include <memory>

#include "AgingRingCacheState.h"

template <typename Key,
          typename Payload,
          typename Hash,
          typename Equal>
    requires std::is_default_constructible_v<Payload>
struct AgingRingCacheEntry {
    std::unique_ptr<Payload> _payload;
    AgingRingCacheState _state {AgingRingCacheState::LOADING};
    uint32_t _pinCount {0};
    uint8_t _age {0};
    std::list<Key>::iterator _ringIt;
};
