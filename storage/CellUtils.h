#pragma once

#include "CellTag.h"

#include "list/ListView.h"
#include "map/MapView.h"

#include "ID.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

namespace db {

/// Helper to call a function on a type given a tag
struct CellTagDispatcher {
    CellTag _tag {CellTag::INVALID};

    auto execute(const auto& executor, const CellView view) const {
        switch (_tag) {
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
            case CellTag::MapView:
                return executor.template operator()<MapView>(view);
            break;

            case CellTag::INVALID:
            break;
        }
        throw FatalException("Unknown CellTag.");
    }
};
    
/// Helpers to convert types to tags
template <typename T>
struct TypeToCellTag;

template <>
struct TypeToCellTag<types::Int64::Primitive> {
    static constexpr CellTag Tag = CellTag::Int;
};

template <>
struct TypeToCellTag<types::UInt64::Primitive> {
    static constexpr CellTag Tag = CellTag::UInt;
};

template <>
struct TypeToCellTag<types::Double::Primitive> {
    static constexpr CellTag Tag = CellTag::Double;
};

template <>
struct TypeToCellTag<types::Bool::Primitive> {
    static constexpr CellTag Tag = CellTag::Bool;
};

template <>
struct TypeToCellTag<types::String::Primitive> {
    static constexpr CellTag Tag = CellTag::String;
};

template <>
struct TypeToCellTag<types::Embedding::Primitive> {
    static constexpr CellTag Tag = CellTag::Embedding;
};

template <>
struct TypeToCellTag<ListView> {
    static constexpr CellTag Tag = CellTag::ListView;
};

template <>
struct TypeToCellTag<PropertyNull> {
    static constexpr CellTag Tag = CellTag::Null;
};

template <>
struct TypeToCellTag<NodeID> {
    static constexpr CellTag Tag = CellTag::NodeID;
};

template <>
struct TypeToCellTag<EdgeID> {
    static constexpr CellTag Tag = CellTag::EdgeID;
};

template <>
struct TypeToCellTag<types::DateTime::Primitive> {
    static constexpr CellTag Tag = CellTag::DateTime;
};

template <>
struct TypeToCellTag<types::Duration::Primitive> {
    static constexpr CellTag Tag = CellTag::Duration;
};

template <>
struct TypeToCellTag<MapView> {
    static constexpr CellTag Tag = CellTag::MapView;
};

}
