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

/// Helper to call a function on a value type given a map value tag
struct MapTagDispatcher {
    MapBufferTypeTag _tag {MapBufferTypeTag::INVALID};

    auto execute(const auto& executor, const MapEntryView view) const {
        switch (_tag) {
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
            case MapBufferTypeTag::INVALID:
            break;
        }
        throw FatalException("Unknown MapBufferTypeTag.");
    }
};

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

}
