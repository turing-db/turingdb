#pragma once

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "BioAssert.h"
#include "ID.h"
#include "RWSpinLock.h"

namespace db {

template <typename T>
concept KeyType = std::same_as<T, LabelID>
               || std::same_as<T, LabelSetID>
               || std::same_as<T, EdgeTypeID>;

template <typename ValueType, typename Key, size_t MaxCount>
class MetadataMap {
public:
    using InternalValueType = std::unique_ptr<ValueType>;
    using Handle = const ValueType*;

    /* Hash the value rather than its address */
    struct Hasher {
        size_t operator()(const Handle& v) const {
            return std::hash<ValueType> {}(*v);
        }
    };

    /* Compare the value rather that its address */
    struct Predicate {
        bool operator()(const Handle& a, const Handle& b) const {
            return *a == *b;
        }
    };

    using ForwardMap = std::unordered_map<Key, InternalValueType>;
    using ReverseMap = std::unordered_map<Handle, Key, Hasher, Predicate>;

    MetadataMap() = default;
    ~MetadataMap() = default;

    MetadataMap(const MetadataMap&) = delete;
    MetadataMap(MetadataMap&&) = delete;
    MetadataMap& operator=(const MetadataMap&) = delete;
    MetadataMap& operator=(MetadataMap&&) = delete;

    [[nodiscard]] Key get(const ValueType& value) const {
        std::shared_lock guard(_lock);

        // Uses custom Hasher and Predicate,
        // the address of value is not actually used
        auto it = _reverseMap.find(&value);
        if (it == _reverseMap.end()) {
            return Key {};
        }
        return it->second;
    }

    [[nodiscard]] const ValueType& getValue(Key id) const {
        std::shared_lock guard(_lock);

        auto it = _forwardMap.find(id);
        msgbioassert(it != _forwardMap.end(),
                     "Metadata object does not exist");
        return *it->second;
    }

    [[nodiscard]] size_t getCount() const {
        std::shared_lock guard(_lock);
        return _forwardMap.size();
    }

    Key getOrCreate(const ValueType& value) {
        std::unique_lock guard(_lock);

        if (!unsafeExists(value)) {
            return unsafeCreate(ValueType(value));
        }

        // Uses custom Hasher and Predicate,
        // the address of value is not actually used
        return _reverseMap.at(&value);
    }

    bool tryCreate(ValueType&& value) {
        std::unique_lock guard(_lock);
        if (!unsafeExists(value)) {
            unsafeCreate(std::move(value));
            return true;
        }

        return false;
    }

    Key create(ValueType&& value) {
        std::unique_lock guard(_lock);
        return unsafeCreate(std::move(value));
    }

    Key create(const ValueType& value) {
        std::unique_lock guard(_lock);
        return unsafeCreate(value);
    }

private:
    mutable RWSpinLock _lock;
    ReverseMap _reverseMap;
    ForwardMap _forwardMap;

    bool unsafeExists(const ValueType& value) const {
        // Uses custom Hasher and Predicate,
        // the address of value is not actually used
        return _reverseMap.find(&value) != _reverseMap.end();
    }

    Key unsafeCreate(ValueType&& value) {
        const auto count = static_cast<Key::Type>(_forwardMap.size());
        msgbioassert(count < MaxCount,
                     "Too many Metadata objects registered");

        const Key nextKey {count};
        auto v = std::make_unique<ValueType>(std::move(value));
        auto* ptr = v.get();

        _forwardMap.emplace(nextKey, std::move(v));
        _reverseMap.emplace(ptr, nextKey);
        return nextKey;
    }

    Key unsafeCreate(const ValueType& value) {
        const auto count = static_cast<Key::Type>(_forwardMap.size());
        msgbioassert(count < MaxCount,
                     "Too many Metadata objects registered");

        const Key nextKey {count};
        auto v = std::make_unique<ValueType>(value);
        auto* ptr = v.get();

        _forwardMap.emplace(nextKey, std::move(v));
        _reverseMap.emplace(ptr, nextKey);
        return nextKey;
    }
};

}
