#pragma once

#include <unordered_map>
#include <functional>
#include <string_view>

#include "StringHasher.h"

template <class K, class V>
struct StringHashMap : std::unordered_map<K, V, StringHasher, std::equal_to<>> {
    using Base = std::unordered_map<K, V, StringHasher, std::equal_to<>>;

    using Base::unordered_map;
    using Base::erase;

    // The standard unordered_map::erase(key) is not heterogeneous, so erasing
    // by string_view requires a transparent lookup followed by an erase by
    // iterator.
    size_t erase(std::string_view key) {
        const auto it = this->find(key);
        if (it == this->end()) {
            return 0;
        }

        this->erase(it);
        return 1;
    }
};
