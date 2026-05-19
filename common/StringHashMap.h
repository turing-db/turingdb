#pragma once

#include <unordered_map>
#include <functional>

#include "StringHasher.h"

template <class K, class V>
struct StringHashMap : std::unordered_map<K, V, StringHasher, std::equal_to<>> {
    using std::unordered_map<K, V, StringHasher, std::equal_to<>>::unordered_map;
};
