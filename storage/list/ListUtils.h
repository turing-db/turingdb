#pragma once

#include "ListView.h"
#include "ListBufferTypeTag.h"

#include "ID.h"

#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

namespace db {

/// Helper to call a function on a type given a tag
struct ListTagDispatcher {
    ListBufferTypeTag _tag {ListBufferTypeTag::INVALID};

    auto execute(const auto& executor, const ListElementView view) const {
        switch (_tag) {
            case ListBufferTypeTag::Int:
                return executor.template operator()<types::Int64::Primitive>(view);
            break;
            case ListBufferTypeTag::UInt:
                return executor.template operator()<types::UInt64::Primitive>(view);
            break;
            case ListBufferTypeTag::Double:
                return executor.template operator()<types::Double::Primitive>(view);
            break;
            case ListBufferTypeTag::Bool:
                return executor.template operator()<types::Bool::Primitive>(view);
            break;
            case ListBufferTypeTag::String:
                return executor.template operator()<types::String::Primitive>(view);
            break;
            case ListBufferTypeTag::Embedding:
                return executor.template operator()<types::Embedding::Primitive>(view);
            break;
            case ListBufferTypeTag::ListView:
                return executor.template operator()<ListView>(view);
            break;
            case ListBufferTypeTag::Null:
                return executor.template operator()<PropertyNull>(view);
            break;
            case ListBufferTypeTag::NodeID:
                return executor.template operator()<NodeID>(view);
            break;
            case ListBufferTypeTag::EdgeID:
                return executor.template operator()<EdgeID>(view);
            break;

            case ListBufferTypeTag::INVALID:
            break;
        }
        throw FatalException("Unknown ListBufferTypeTag.");
    }
};
    
/// Helpers to convert types to tags
template <typename T>
struct TypeToListBufferTag;

template <>
struct TypeToListBufferTag<types::Int64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Int;
};

template <>
struct TypeToListBufferTag<types::UInt64::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::UInt;
};

template <>
struct TypeToListBufferTag<types::Double::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Double;
};

template <>
struct TypeToListBufferTag<types::Bool::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Bool;
};

template <>
struct TypeToListBufferTag<types::String::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::String;
};

template <>
struct TypeToListBufferTag<types::Embedding::Primitive> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Embedding;
};

template <>
struct TypeToListBufferTag<ListView> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::ListView;
};

template <>
struct TypeToListBufferTag<PropertyNull> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::Null;
};

template <>
struct TypeToListBufferTag<NodeID> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::NodeID;
};

template <>
struct TypeToListBufferTag<EdgeID> {
    static constexpr ListBufferTypeTag Tag = ListBufferTypeTag::EdgeID;
};

}
