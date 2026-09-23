#include "MapHash.h"

#include <stdint.h>
#include <string.h>

#include <bit>
#include <functional>
#include <string_view>

#include "MapBufferTypeTag.h"
#include "MapEntryView.h"

#include "ID.h"
#include "list/EncodedList.h"
#include "list/ListHash.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

using namespace db;

namespace {

size_t combine(size_t seed, size_t value) {
    return seed * 31 + value;
}

template <typename T>
size_t hashValue(const T& value) {
    return std::hash<T> {}(value);
}

size_t hashMapValue(MapEntryView entry) {
    const MapBufferTypeTag tag = entry.getValueTag();
    const size_t seed = static_cast<size_t>(tag);

    switch (tag) {
        case MapBufferTypeTag::Int:
            return hashNumber(static_cast<double>(entry.getValueAs<types::Int64::Primitive>()));
        break;
        case MapBufferTypeTag::UInt:
            return hashNumber(static_cast<double>(entry.getValueAs<types::UInt64::Primitive>()));
        break;
        case MapBufferTypeTag::Double:
            return hashNumber(entry.getValueAs<types::Double::Primitive>());
        break;
        case MapBufferTypeTag::Bool:
            return combine(seed, hashValue(entry.getValueAs<types::Bool::Primitive>()));
        break;
        case MapBufferTypeTag::String:
            return combine(seed, hashValue(entry.getValueAs<types::String::Primitive>()));
        break;
        case MapBufferTypeTag::NodeID:
            return combine(seed, hashValue(entry.getValueAs<NodeID>().getValue()));
        break;
        case MapBufferTypeTag::EdgeID:
            return combine(seed, hashValue(entry.getValueAs<EdgeID>().getValue()));
        break;
        case MapBufferTypeTag::DateTime:
            return combine(seed, hashValue(entry.getValueAs<types::DateTime::Primitive>()));
        break;
        case MapBufferTypeTag::Embedding: {
            size_t hash = seed;
            for (const float value : entry.getValueAs<types::Embedding::Primitive>()) {
                hash = combine(hash, hashValue(value));
            }
            return hash;
        }
        break;
        case MapBufferTypeTag::ListView:
            return combine(seed, hashList(entry.getValueAs<ListView>()));
        break;
        case MapBufferTypeTag::MapView:
            return combine(seed, hashMap(entry.getValueAs<MapView>()));
        break;
        case MapBufferTypeTag::Null:
        case MapBufferTypeTag::INVALID:
            return seed;
        break;
    }

    return seed;
}

bool sameMapValue(MapEntryView lhs, MapEntryView rhs) {
    const MapBufferTypeTag tag = lhs.getValueTag();
    if (tag != rhs.getValueTag()) {
        return false;
    }

    switch (tag) {
        case MapBufferTypeTag::Int:
            return lhs.getValueAs<types::Int64::Primitive>() == rhs.getValueAs<types::Int64::Primitive>();
        break;
        case MapBufferTypeTag::UInt:
            return lhs.getValueAs<types::UInt64::Primitive>() == rhs.getValueAs<types::UInt64::Primitive>();
        break;
        case MapBufferTypeTag::Double:
            return std::bit_cast<uint64_t>(lhs.getValueAs<types::Double::Primitive>())
                == std::bit_cast<uint64_t>(rhs.getValueAs<types::Double::Primitive>());
        break;
        case MapBufferTypeTag::Bool:
            return lhs.getValueAs<types::Bool::Primitive>() == rhs.getValueAs<types::Bool::Primitive>();
        break;
        case MapBufferTypeTag::String:
            return lhs.getValueAs<types::String::Primitive>() == rhs.getValueAs<types::String::Primitive>();
        break;
        case MapBufferTypeTag::NodeID:
            return lhs.getValueAs<NodeID>() == rhs.getValueAs<NodeID>();
        break;
        case MapBufferTypeTag::EdgeID:
            return lhs.getValueAs<EdgeID>() == rhs.getValueAs<EdgeID>();
        break;
        case MapBufferTypeTag::DateTime:
            return lhs.getValueAs<types::DateTime::Primitive>() == rhs.getValueAs<types::DateTime::Primitive>();
        break;
        case MapBufferTypeTag::Embedding: {
            const types::Embedding::Primitive lhsValues = lhs.getValueAs<types::Embedding::Primitive>();
            const types::Embedding::Primitive rhsValues = rhs.getValueAs<types::Embedding::Primitive>();

            return lhsValues.size() == rhsValues.size()
                && memcmp(lhsValues.data(), rhsValues.data(), lhsValues.size_bytes()) == 0;
        }
        break;
        case MapBufferTypeTag::ListView:
            return EncodedList(lhs.getValueAs<ListView>()) == EncodedList(rhs.getValueAs<ListView>());
        break;
        case MapBufferTypeTag::MapView:
            return sameMap(lhs.getValueAs<MapView>(), rhs.getValueAs<MapView>());
        break;
        case MapBufferTypeTag::Null:
        case MapBufferTypeTag::INVALID:
            return true;
        break;
    }

    return false;
}

}

size_t db::hashMap(MapView map) {
    size_t hash = map.size();

    for (const MapEntryView entry : map) {
        hash = combine(hash, hashValue(entry.getKey()));
        hash = combine(hash, hashMapValue(entry));
    }

    return hash;
}

bool db::sameMap(MapView lhs, MapView rhs) {
    if (lhs.size() != rhs.size()) {
        return false;
    }

    const std::span<const MapEntryView> lhsEntries = lhs.entries();
    const std::span<const MapEntryView> rhsEntries = rhs.entries();

    for (size_t i = 0; i < lhsEntries.size(); i++) {
        const MapEntryView lhsEntry = lhsEntries[i];
        const MapEntryView rhsEntry = rhsEntries[i];

        if (lhsEntry.getKey() != rhsEntry.getKey() || !sameMapValue(lhsEntry, rhsEntry)) {
            return false;
        }
    }

    return true;
}
