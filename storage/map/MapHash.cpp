#include "MapHash.h"

#include <stdint.h>
#include <string.h>

#include <bit>
#include <functional>
#include <string_view>

#include "CellTag.h"
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
    const CellTag tag = entry.getValue().getTag();
    const size_t seed = static_cast<size_t>(tag);

    switch (tag) {
        case CellTag::Int:
            return hashNumber(static_cast<double>(entry.getValue().getAs<types::Int64::Primitive>()));
        break;
        case CellTag::UInt:
            return hashNumber(static_cast<double>(entry.getValue().getAs<types::UInt64::Primitive>()));
        break;
        case CellTag::Double:
            return hashNumber(entry.getValue().getAs<types::Double::Primitive>());
        break;
        case CellTag::Bool:
            return combine(seed, hashValue(entry.getValue().getAs<types::Bool::Primitive>()));
        break;
        case CellTag::String:
            return combine(seed, hashValue(entry.getValue().getAs<types::String::Primitive>()));
        break;
        case CellTag::NodeID:
            return combine(seed, hashValue(entry.getValue().getAs<NodeID>().getValue()));
        break;
        case CellTag::EdgeID:
            return combine(seed, hashValue(entry.getValue().getAs<EdgeID>().getValue()));
        break;
        case CellTag::DateTime:
            return combine(seed, hashValue(entry.getValue().getAs<types::DateTime::Primitive>()));
        break;
        case CellTag::Duration:
            return combine(seed, hashValue(entry.getValue().getAs<types::Duration::Primitive>()));
        break;
        case CellTag::Embedding: {
            size_t hash = seed;
            for (const float value : entry.getValue().getAs<types::Embedding::Primitive>()) {
                hash = combine(hash, hashValue(value));
            }
            return hash;
        }
        break;
        case CellTag::ListView:
            return combine(seed, hashList(entry.getValue().getAs<ListView>()));
        break;
        case CellTag::MapView:
            return combine(seed, hashMap(entry.getValue().getAs<MapView>()));
        break;
        case CellTag::Null:
        case CellTag::INVALID:
            return seed;
        break;
    }

    return seed;
}

bool sameMapValue(MapEntryView lhs, MapEntryView rhs) {
    const CellTag tag = lhs.getValue().getTag();
    if (tag != rhs.getValue().getTag()) {
        return false;
    }

    switch (tag) {
        case CellTag::Int:
            return lhs.getValue().getAs<types::Int64::Primitive>() == rhs.getValue().getAs<types::Int64::Primitive>();
        break;
        case CellTag::UInt:
            return lhs.getValue().getAs<types::UInt64::Primitive>() == rhs.getValue().getAs<types::UInt64::Primitive>();
        break;
        case CellTag::Double:
            return std::bit_cast<uint64_t>(lhs.getValue().getAs<types::Double::Primitive>())
                == std::bit_cast<uint64_t>(rhs.getValue().getAs<types::Double::Primitive>());
        break;
        case CellTag::Bool:
            return lhs.getValue().getAs<types::Bool::Primitive>() == rhs.getValue().getAs<types::Bool::Primitive>();
        break;
        case CellTag::String:
            return lhs.getValue().getAs<types::String::Primitive>() == rhs.getValue().getAs<types::String::Primitive>();
        break;
        case CellTag::NodeID:
            return lhs.getValue().getAs<NodeID>() == rhs.getValue().getAs<NodeID>();
        break;
        case CellTag::EdgeID:
            return lhs.getValue().getAs<EdgeID>() == rhs.getValue().getAs<EdgeID>();
        break;
        case CellTag::DateTime:
            return lhs.getValue().getAs<types::DateTime::Primitive>() == rhs.getValue().getAs<types::DateTime::Primitive>();
        break;
        case CellTag::Duration:
            return lhs.getValue().getAs<types::Duration::Primitive>() == rhs.getValue().getAs<types::Duration::Primitive>();
        break;
        case CellTag::Embedding: {
            const types::Embedding::Primitive lhsValues = lhs.getValue().getAs<types::Embedding::Primitive>();
            const types::Embedding::Primitive rhsValues = rhs.getValue().getAs<types::Embedding::Primitive>();

            return lhsValues.size() == rhsValues.size()
                && memcmp(lhsValues.data(), rhsValues.data(), lhsValues.size_bytes()) == 0;
        }
        break;
        case CellTag::ListView:
            return EncodedList(lhs.getValue().getAs<ListView>()) == EncodedList(rhs.getValue().getAs<ListView>());
        break;
        case CellTag::MapView:
            return sameMap(lhs.getValue().getAs<MapView>(), rhs.getValue().getAs<MapView>());
        break;
        case CellTag::Null:
        case CellTag::INVALID:
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
