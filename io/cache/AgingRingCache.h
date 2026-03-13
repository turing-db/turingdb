#pragma once

#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <list>

#include "AgingRingCacheResult.h"
#include "AgingRingCacheEntry.h"
#include "AgingRingCacheHandle.h"

#include "BioAssert.h"

template <typename Key,
          typename Payload,
          typename Hash = std::hash<Key>,
          typename Equal = std::equal_to<Key>>
    requires std::is_default_constructible_v<Payload>
class AgingRingCache {
public:
    using OnLoadFn = bool (*)(const Key&, Payload&, void*);
    using OnEvictFn = bool (*)(const Key&, Payload&);
    using CalculateMemoryUsageFn = size_t (*)(const Payload&);

    using OnLoadCallback = std::function<bool(const Key&, Payload&, void*)>;
    using OnEvictCallback = std::function<bool(const Key&, Payload&)>;
    using CalculateMemoryUsageCallback = std::function<size_t(const Payload&)>;

    using Entry = AgingRingCacheEntry<Key, Payload, Hash, Equal>;
    using HashMap = std::unordered_map<Key, Entry, Hash, Equal>;
    using Ring = std::list<Key>;
    using RingIterator = std::list<Key>::iterator;
    using Handle = AgingRingCacheHandle<Key, Payload, Hash, Equal>;

    AgingRingCache() = default;

    ~AgingRingCache() {
        shutdown();
    }

    AgingRingCache(const AgingRingCache&) = delete;
    AgingRingCache(AgingRingCache&&) = delete;
    AgingRingCache& operator=(const AgingRingCache&) = delete;
    AgingRingCache& operator=(AgingRingCache&&) = delete;

    void setOnLoad(const OnLoadCallback& callback) {
        _onLoad = callback;
    }

    void setOnEvict(const OnEvictCallback& callback) {
        _onEvict = callback;
    }

    void setCalculateMemoryUsage(const CalculateMemoryUsageCallback& callback) {
        _calculateMemoryUsage = callback;
    }

    void setMaxMemUsage(size_t maxMemUsage) {
        _maxMemUsage = maxMemUsage;
    }

    template <typename... Args>
    AgingRingCacheResult<Handle> acquire(const Key& key, void* data = nullptr) {
        for (;;) {
            std::unique_lock lock(_mutex);

            if (_terminating) {
                return AgingRingCacheError::result(AgingRingCacheErrorCode::TERMINATING);
            }

            const auto it = _map.find(key);

            /* Case 1: Cache miss
             *         Create a new entry (LOADING state) */
            if (it == _map.end()) {
                // Step 1: Prepare a new and add a new entry to the map
                auto payload = std::make_unique<Payload>();
                Payload* payloadPtr = payload.get();

                const auto [newIt, inserted] = _map.emplace(
                    key,
                    Entry {
                        ._payload = std::move(payload),
                        ._state = AgingRingCacheState::LOADING,
                    });

                Entry* entryPtr = &newIt->second;

                // Step 2: Load the payload but keep the cache unlocked during io
                _inFlightIoCount++;

                lock.unlock();
                const auto loadRes = _onLoad(key, *payloadPtr, data);
                lock.lock();

                onIoFinishedLocked();

                // If failed to load, remove the entry and return the error
                if (!loadRes) {
                    _map.erase(key);
                    _stateCv.notify_all();

                    return AgingRingCacheError::result(AgingRingCacheErrorCode::COULD_NOT_LOAD);
                }

                // If terminating, remove the entry and return the error
                if (_terminating) {
                    _map.erase(key);
                    _stateCv.notify_all();

                    return AgingRingCacheError::result(AgingRingCacheErrorCode::TERMINATING);
                }

                // Step 3: Successful load, publish as Resident and return the handle
                entryPtr->_pinCount = 1;
                _totalPinCount++;
                putInRingLocked(key, *entryPtr);

                Handle h;
                h._cache = this;
                h._entry = entryPtr;

                lock.unlock();
                evictToLimit(); // Ignore eviction errors

                return h;
            }

            Entry& entry = it->second;

            /* Case 2: Found an entry, but it's currently being loaded by another thread
             *         Wait until it's done and retry */
            if (entry._state == AgingRingCacheState::LOADING) {
                _stateCv.wait(lock, [&] {
                    const auto it2 = _map.find(key);
                    if (it2 == _map.end()) {
                        // This can happen in case an error occured during the loading
                        return true;
                    }

                    return it2->second._state != AgingRingCacheState::LOADING;
                });

                // The state of the entry has changed, check if the loading succeeded
                if (!_map.contains(key)) {
                    return AgingRingCacheError::result(AgingRingCacheErrorCode::COULD_NOT_LOAD);
                }

                continue; // -> Retry
            }

            /* Case 3: Found an entry, but it's currently being evicted by another thread
             *         Wait until it's done and retry
             *
             * NOTE: An optimization could be to ask the other thread to abort the eviction */
            if (entry._state == AgingRingCacheState::EVICTING) {
                _stateCv.wait(lock, [&] {
                    const auto it2 = _map.find(key);
                    if (it2 == _map.end()) {
                        // This should never happen, but just in case
                        return true;
                    }

                    return it2->second._state != AgingRingCacheState::EVICTING;
                });

                continue; // -> Retry
            }

            /* Case 4: Cache hit
             *         Pin and return the entry */
            bioassert(entry._state == AgingRingCacheState::RESIDENT, "Unexpected cache entry state");
            entry._pinCount++;
            _totalPinCount++;
            entry._age = _maxAge;

            Handle h;
            h._cache = this;
            h._entry = &entry;

            return h;
        }
    }

    /** @brief Saves all resident entries and shuts down the cache.
     *
     * Blocks until all pinned entries are released, then evicts all remaining entries.
     * After this call, acquire() will return TERMINATING errors.
     */
    void shutdown() {
        {
            std::unique_lock lock(_mutex);

            if (_terminating) {
                return;
            }

            _terminating = true;
            _stateCv.notify_all();
            _quiescentCv.wait(lock, [this] {
                return _totalPinCount == 0 && _inFlightIoCount == 0;
            });
        }

        // Evict all remaining resident entries outside the lock
        for (;;) {
            std::optional<Key> victimKey;
            std::optional<Payload*> victimPayload;

            {
                std::unique_lock lock(_mutex);

                victimKey = pickAnyResidentLocked();
                if (!victimKey) {
                    break;
                }

                const auto it = _map.find(*victimKey);
                bioassert(it != _map.end(), "Could not find key in map");

                Entry& entry = it->second;
                victimPayload = entry._payload.get();
            }

            _onEvict(*victimKey, **victimPayload);

            {
                std::unique_lock lock(_mutex);

                // Find the entry again since the map may be have been modified
                // during the eviction callback execution (invalidated iterators)
                const auto it = _map.find(*victimKey);
                if (it != _map.end()) {
                    _map.erase(it);
                }
            }
        }
    }

    /** @brief Evicts a single entry from the cache. */
    void tryEvict(const Key& key) {
        std::unique_lock lock(_mutex);

        auto it = _map.find(key);
        if (it == _map.end()) {
            return;
        }

        Entry& entry = it->second;
        bioassert(entry._state == AgingRingCacheState::EVICTING, "Unexpected cache entry state");
        Payload& victimPayload = *entry._payload;
        _inFlightIoCount++;

        lock.unlock();
        const auto evictRes = _onEvict(key, victimPayload);
        lock.lock();

        onIoFinishedLocked();

        if (!evictRes) {
            // Put back the victim in the cache since the eviction failed and return an error

            it = _map.find(key);
            if (it == _map.end()) {
                // Should never happen, entry disappeared while we were saving it
                return;
            }

            Entry& entry = it->second;
            putInRingLocked(key, entry);

            return;
        }

        it = _map.find(key);
        if (it == _map.end()) {
            // Should never happen, entry disappeared while we were erasing it
            return;
        }

        _map.erase(it);
        _stateCv.notify_all();
    }

private:
    friend class AgingRingCacheHandle<Key, Payload, Hash, Equal>;

    /** @brief Hash map for entries in the cache. */
    HashMap _map;

    /** @brief List used as storage for the ring of the aging CLOCK-like eviction algorithm.
     *
     * Stores resident entries only.
     * */
    Ring _ring;

    /** @brief Hand (cursor) of the ring.
     *
     * Walks the resident ring for eviction.
     * */
    RingIterator _clockHand;

    /** @brief Mutex that locks the hash map and the ring. */
    mutable std::mutex _mutex;

    /** @brief Condition variable used to wait on key state changes safely. */
    std::condition_variable _stateCv;

    /** @brief Condition variable used to wait on IO completion safely. */
    std::condition_variable _quiescentCv;

    /** @brief Maximum memory usage of the entire cache. */
    size_t _maxMemUsage {64ull * 1024 * 1024}; // 64MB

    /** @brief Maximum age of an entry in the cache.
     *
     * Replaces the "referenced" boolean typically used in regular CLOCK algorithms.
     * */
    uint8_t _maxAge {3};

    /** @brief True if the cache is terminating. */
    bool _terminating {false};

    /** @brief Total count of currently pinned handles across all entries. */
    uint32_t _totalPinCount {0};

    /** @brief Total count of in flight IO operations. */
    uint32_t _inFlightIoCount {0};

    /** @brief Default invalid payload callback. */
    static constexpr const OnLoadFn _invalidOnLoad = [](const Key&, Payload&, void*) -> bool {
        throw std::runtime_error("AgingRingCache error: On load callback is not set");
    };

    /** @brief Default invalid payload callback. */
    static constexpr const OnEvictFn _invalidOnEvict = [](const Key&, Payload&) -> bool {
        throw std::runtime_error("AgingRingCache error: On evict callback is not set");
    };

    /** @brief Default memory usage estimation callback. */
    static constexpr const CalculateMemoryUsageFn _defaultCalculateMemoryUsage = [](const Payload&) -> size_t {
        return sizeof(Payload);
    };

    /** @brief Callback called when a payload is added to the cache. */
    OnLoadCallback _onLoad {_invalidOnLoad};

    /** @brief Callback called when a payload is evicted from the cache. */
    OnEvictCallback _onEvict {_invalidOnEvict};

    /** @brief Callback called to estimate the memory usage of a payload. */
    CalculateMemoryUsageCallback _calculateMemoryUsage {_defaultCalculateMemoryUsage};

    void release(Handle& handle) {
        std::unique_lock lock(_mutex);

        if (handle._entry == nullptr || handle._entry->_state != AgingRingCacheState::RESIDENT) {
            return;
        }

        handle._entry->_pinCount--;
        _totalPinCount--;

        if (_totalPinCount == 0) {
            _quiescentCv.notify_all();
        }
    }

    /** @brief Returns the total memory usage of all resident payloads.
     *
     * Must be called with _mutex held.
     */
    size_t calculateTotalMemUsageLocked() const {
        size_t total = 0;

        for (const auto& [key, entry] : _map) {
            if (entry._state == AgingRingCacheState::RESIDENT && entry._payload) {
                total += _calculateMemoryUsage(*entry._payload);
            }
        }

        return total;
    }

    AgingRingCacheResult<void> evictToLimit() {
        for (;;) {
            std::optional<Key> victimKey;
            std::optional<Payload*> victimPayload;

            // Step 1: Pick a victim
            {
                const std::unique_lock lock(_mutex);

                const size_t memUsage = calculateTotalMemUsageLocked();

                if (memUsage <= _maxMemUsage) {
                    return {};
                }

                victimKey = pickVictimLocked();

                if (!victimKey) {
                    // No victim found, nothing to evict
                    return AgingRingCacheError::result(AgingRingCacheErrorCode::NOTHING_TO_EVICT);
                }

                const auto it = _map.find(*victimKey);
                if (it == _map.end()) {
                    // Should never happen
                    return AgingRingCacheError::result(AgingRingCacheErrorCode::UNKNOWN);
                }

                Entry& entry = it->second;
                bioassert(entry._state == AgingRingCacheState::EVICTING, "Unexpected cache entry state");
                victimPayload = entry._payload.get();
                _inFlightIoCount++;
            }

            // Step 2: Save the victim on disk outside the lock
            const auto evictRes = _onEvict(*victimKey, **victimPayload);

            const std::unique_lock lock(_mutex);
            onIoFinishedLocked();

            if (!evictRes) {
                // Put back the victim in the cache since the eviction failed and return an error

                const auto it = _map.find(*victimKey);
                if (it == _map.end()) {
                    // Should never happen, entry disappeared while we were saving it
                    return AgingRingCacheError::result(AgingRingCacheErrorCode::UNKNOWN);
                }

                Entry& entry = it->second;
                putInRingLocked(*victimKey, entry);

                return AgingRingCacheError::result(AgingRingCacheErrorCode::COULD_NOT_EVICT);
            }

            // Step 3: erase the victim from the map
            const auto it = _map.find(*victimKey);
            if (it == _map.end()) {
                // Should never happen, entry disappeared while we were erasing it
                return AgingRingCacheError::result(AgingRingCacheErrorCode::UNKNOWN);
            }

            _map.erase(it);
            _stateCv.notify_all();
        }
    }

    std::optional<Key> pickVictim() {
        std::unique_lock lock(_mutex);
        return pickVictimLocked();
    }

    std::optional<Key> pickVictimLocked() {
        if (_ring.empty()) {
            return std::nullopt;
        }

        if (_clockHand == _ring.end()) {
            // Wrap around
            _clockHand = _ring.begin();
        }

        const size_t attempts = _ring.size() * static_cast<size_t>(_maxAge + 1);

        for (size_t i = 0; i < attempts; i++) {
            const auto ringIt = _clockHand;
            const auto next = std::next(ringIt);

            _clockHand = (next == _ring.end()) ? _ring.begin() : next;

            const Key& key = *ringIt;
            const auto it = _map.find(key);
            bioassert(it != _map.end(), "Could not find key in map");

            Entry& entry = it->second;
            bioassert(entry._state == AgingRingCacheState::RESIDENT, "Unexpected cache entry state");

            // If in use, skip
            if (entry._pinCount > 0) {
                continue;
            }

            // If age > 0, give another chance
            if (entry._age > 0) {
                entry._age--;
                continue;
            }

            // Found a victim
            entry._state = AgingRingCacheState::EVICTING;
            _ring.erase(ringIt);

            if (_ring.empty()) {
                _clockHand = _ring.end();
            }

            return it->first;
        }

        // No suitable victim found

        return std::nullopt;
    }

    /** @brief Picks any resident (unpinned) entry for eviction during shutdown.
     *
     * Must be called with _mutex held.
     */
    std::optional<Key> pickAnyResidentLocked() {
        for (auto it = _ring.begin(); it != _ring.end(); ++it) {
            const Key& key = *it;
            const auto mapIt = _map.find(key);
            bioassert(mapIt != _map.end(), "Could not find key in map");

            Entry& entry = mapIt->second;
            if (entry._pinCount == 0) {
                entry._state = AgingRingCacheState::EVICTING;

                if (_clockHand == it) {
                    _clockHand = _ring.erase(it);
                    if (_clockHand == _ring.end() && !_ring.empty()) {
                        _clockHand = _ring.begin();
                    }
                } else {
                    _ring.erase(it);
                }

                return mapIt->first;
            }
        }
        return std::nullopt;
    }

    void putInRingLocked(const Key& key, Entry& entry) {
        entry._state = AgingRingCacheState::RESIDENT;
        entry._age = _maxAge;
        _ring.push_back(key);
        entry._ringIt = std::prev(_ring.end());

        if (_ring.size() == 1) {
            _clockHand = _ring.begin();
        }

        _stateCv.notify_all();
    }

    void onIoFinishedLocked() {
        bioassert(_inFlightIoCount > 0, "Invalid in-flight io count");
        _inFlightIoCount--;

        if (_inFlightIoCount == 0 && _totalPinCount == 0) {
            _quiescentCv.notify_all();
        }
    }
};
