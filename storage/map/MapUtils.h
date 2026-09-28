#pragma once

#include "MapView.h"
#include "MapEntryView.h"
#include "MapBufferTypeTag.h"

#include "ID.h"
#include "list/ListView.h"
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
auto dispatchMapTag(const MapBufferTypeTag tag, const auto& executor, const MapEntryView view) {
    switch (tag) {
        case MapBufferTypeTag::Int:
            return executor.template operator()<types::Int64::Primitive>(view);
        break;
        case MapBufferTypeTag::UInt:
            return executor.template operator()<types::UInt64::Primitive>(view);
        break;
        case MapBufferTypeTag::Double:
            return executor.template operator()<types::Double::Primitive>(view);
        break;
        case MapBufferTypeTag::Bool:
            return executor.template operator()<types::Bool::Primitive>(view);
        break;
        case MapBufferTypeTag::String:
            return executor.template operator()<types::String::Primitive>(view);
        break;
        case MapBufferTypeTag::Embedding:
            return executor.template operator()<types::Embedding::Primitive>(view);
        break;
        case MapBufferTypeTag::ListView:
            return executor.template operator()<ListView>(view);
        break;
        case MapBufferTypeTag::MapView:
            return executor.template operator()<MapView>(view);
        break;
        case MapBufferTypeTag::Null:
            return executor.template operator()<PropertyNull>(view);
        break;
        case MapBufferTypeTag::NodeID:
            return executor.template operator()<NodeID>(view);
        break;
        case MapBufferTypeTag::EdgeID:
            return executor.template operator()<EdgeID>(view);
        break;
        case MapBufferTypeTag::DateTime:
            return executor.template operator()<types::DateTime::Primitive>(view);
        break;
        case MapBufferTypeTag::Duration:
            return executor.template operator()<types::Duration::Primitive>(view);
        break;

        case MapBufferTypeTag::INVALID:
        break;
    }

    throw FatalException("Unknown MapBufferTypeTag.");
}

/// Call the executor with the type the entry's own tag names.
auto dispatchMapEntry(const auto& executor, const MapEntryView entry) {
    return dispatchMapTag(entry.getValueTag(), executor, entry);
}

/// Helpers to convert types to map value tags
template <typename T>
struct TypeToMapBufferTag;

template <>
struct TypeToMapBufferTag<types::Int64::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Int;
};

template <>
struct TypeToMapBufferTag<types::UInt64::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::UInt;
};

template <>
struct TypeToMapBufferTag<types::Double::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Double;
};

template <>
struct TypeToMapBufferTag<types::Bool::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Bool;
};

template <>
struct TypeToMapBufferTag<types::String::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::String;
};

template <>
struct TypeToMapBufferTag<types::Embedding::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Embedding;
};

template <>
struct TypeToMapBufferTag<ListView> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::ListView;
};

template <>
struct TypeToMapBufferTag<MapView> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::MapView;
};

template <>
struct TypeToMapBufferTag<PropertyNull> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Null;
};

template <>
struct TypeToMapBufferTag<NodeID> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::NodeID;
};

template <>
struct TypeToMapBufferTag<EdgeID> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::EdgeID;
};

template <>
struct TypeToMapBufferTag<types::DateTime::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::DateTime;
};

template <>
struct TypeToMapBufferTag<types::Duration::Primitive> {
    static constexpr MapBufferTypeTag Tag = MapBufferTypeTag::Duration;
};

}
