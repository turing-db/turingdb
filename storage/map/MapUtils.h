#pragma once

#include "MapView.h"
#include "MapEntryView.h"
#include "CellTag.h"

#include "ID.h"
#include "list/ListView.h"
#include "list/ListUtils.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "FatalException.h"

namespace db {

// Entries are meant to be key-sorted, but MapBuffer::insert does not sort - only the two
// producers feeding it do - so a scan is the reading that holds for every producer. Maps
// are small enough that it costs nothing; revisit if that stops being true.
inline bool findMapEntry(const MapView map, const std::string_view key, MapEntryView& entry) {
    for (const MapEntryView candidate : map) {
        if (candidate.getKey() == key) {
            entry = candidate;
            return true;
        }
    }

    return false;
}

// Call the executor with the type a map value tag names 
auto dispatchMapTag(const CellTag tag, const auto& executor, const MapEntryView view) {
    switch (tag) {
        case CellTag::Int:
            return executor.template operator()<types::Int64::Primitive>(view);
        break;
        case CellTag::UInt:
            return executor.template operator()<types::UInt64::Primitive>(view);
        break;
        case CellTag::Double:
            return executor.template operator()<types::Double::Primitive>(view);
        break;
        case CellTag::Bool:
            return executor.template operator()<types::Bool::Primitive>(view);
        break;
        case CellTag::String:
            return executor.template operator()<types::String::Primitive>(view);
        break;
        case CellTag::Embedding:
            return executor.template operator()<types::Embedding::Primitive>(view);
        break;
        case CellTag::ListView:
            return executor.template operator()<ListView>(view);
        break;
        case CellTag::MapView:
            return executor.template operator()<MapView>(view);
        break;
        case CellTag::Null:
            return executor.template operator()<PropertyNull>(view);
        break;
        case CellTag::NodeID:
            return executor.template operator()<NodeID>(view);
        break;
        case CellTag::EdgeID:
            return executor.template operator()<EdgeID>(view);
        break;
        case CellTag::DateTime:
            return executor.template operator()<types::DateTime::Primitive>(view);
        break;
        case CellTag::Duration:
            return executor.template operator()<types::Duration::Primitive>(view);
        break;

        case CellTag::INVALID:
        break;
    }

    throw FatalException("Unknown CellTag.");
}

/// Call the executor with the type the entry's own tag names.
auto dispatchMapEntry(const auto& executor, const MapEntryView entry) {
    return dispatchMapTag(entry.getValueTag(), executor, entry);
}

}
