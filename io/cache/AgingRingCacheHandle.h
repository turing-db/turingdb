#pragma once

#include <type_traits>

#include "AgingRingCacheEntry.h"

template <typename Key,
          typename Payload,
          typename Hash,
          typename Equal>
    requires std::is_default_constructible_v<Payload>
class AgingRingCache;

template <typename Key,
          typename Payload,
          typename Hash,
          typename Equal>
    requires std::is_default_constructible_v<Payload>
class AgingRingCacheHandle {
public:
    using Cache = AgingRingCache<Key, Payload, Hash, Equal>;
    using Entry = AgingRingCacheEntry<Key, Payload, Hash, Equal>;

    AgingRingCacheHandle() = default;

    ~AgingRingCacheHandle() {
        if (!_cache) {
            return;
        }

        _cache->release(*this);
    }

    AgingRingCacheHandle(AgingRingCacheHandle&& other) noexcept
        : _cache(other._cache),
          _entry(other._entry) {
        other._cache = nullptr;
        other._entry = nullptr;
    }

    AgingRingCacheHandle& operator=(AgingRingCacheHandle&& other) noexcept {
        if (&other == this) {
            return *this;
        }

        if (_cache) {
            _cache->release(*this);
        }

        _cache = other._cache;
        _entry = other._entry;

        other._cache = nullptr;
        other._entry = nullptr;

        return *this;
    }

    AgingRingCacheHandle(const AgingRingCacheHandle&) = delete;
    AgingRingCacheHandle& operator=(const AgingRingCacheHandle&) = delete;

    Payload& get() noexcept { return *_entry->_payload; }
    Payload* operator->() noexcept { return _entry->_payload.get(); }
    Payload& operator*() noexcept { return get(); }
    const Payload& get() const noexcept { return *_entry->_payload; }
    const Payload* operator->() const noexcept { return _entry->_payload.get(); }
    const Payload& operator*() const noexcept { return get(); }

    explicit operator bool() const noexcept { return _entry != nullptr; }

private:
    friend Cache;

    Cache* _cache {nullptr};
    Entry* _entry {nullptr};

    AgingRingCacheHandle(Cache* cache, Entry* entry)
        : _cache(cache),
        _entry(entry)
    {
    }
};

