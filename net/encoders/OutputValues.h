#pragma once

#include <string_view>
#include <type_traits>

#include "GraphPath.h"
#include "ID.h"
#include "ListElementView.h"
#include "OptionalLike.h"
#include "TypeUtils.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitHash.h"

#include "Panic.h"

namespace db {

class EntityList;

template <typename T>
struct IsHash : std::false_type {};

template <int N>
struct IsHash<TemplateCommitHash<N>> : std::true_type {};

template <typename T>
concept Hash = IsHash<T>::value;

template <typename T>
concept IDLike = IsID<T>::value;

template <typename T>
concept IsUInt64 = IsID<T>::value
                || std::unsigned_integral<T>
                || IsHash<T>::value
                || OptionalUnsignedInteger<T>;

template <typename T>
concept IsInt64 = std::signed_integral<T>
               || OptionalSignedInteger<T>;

template <typename T>
concept IsFloat64 = std::floating_point<T>
                 || OptionalFloatingPoint<T>;

template <typename T>
concept IsString = std::is_convertible_v<T, std::string_view>
                || OptionalString<T>;

template <typename T>
concept IsValueType = std::is_same_v<T, ValueType>;

template <typename T>
concept IsBool = std::is_same_v<T, CustomBool>
              || std::is_same_v<T, bool>
              || OptionalBool<T>;

template <typename T>
concept IsPath = std::is_same_v<T, Path>;

template <typename T>
concept IsList = std::is_same_v<T, EntityList> || std::is_same_v<T, ListView>;

template <typename T>
concept IsListElement = std::is_same_v<T, ListElementView>;

template <typename T>
concept IsNull = std::is_same_v<T, PropertyNull>;

template <typename T>
concept IsEmbedding = std::is_same_v<TypeUtils::unwrap_optional_t<T>, types::Embedding::Primitive>;

struct ColumnTypeGenerator {
    std::string& _name;

    template <template <typename> typename U, typename T>
    void operator()(const U<T>*) {
        if constexpr (IsEmbedding<T>) {
            _name = fmt::format("Embedding");
        } else if constexpr (IsUInt64<T>) {
            _name = fmt::format("UInt64");
        } else if constexpr (IsInt64<T>) {
            _name = fmt::format("Int64");
        } else if constexpr (IsFloat64<T>) {
            _name = fmt::format("Double");
        } else if constexpr (IsString<T>) {
            _name = fmt::format("String");
        } else if constexpr (IsValueType<T>) {
            _name = fmt::format("String");
        } else if constexpr (IsBool<T>) {
            _name = fmt::format("Bool");
        } else if constexpr (IsPath<T>) {
            _name = fmt::format("Path");
        } else if constexpr (IsNull<T>) {
            _name = fmt::format("NULL");
        } else if constexpr (IsList<T>) {
            _name = fmt::format("List");
        } else if constexpr (IsListElement<T>) {
            _name = fmt::format("ListElement");
        } else {
            COMPILE_ERROR("Unknown column type");
        }
    }
};

}
