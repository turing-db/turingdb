#pragma once

#include "Column.h"
#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ColumnOptVector.h"
#include "ColumnKind.h"
#include "metadata/PropertyType.h"

#include "FatalException.h"

namespace db {

namespace {
// Declaritive implementation of ColumnTypeFromKind
template <typename... Types>
struct TypeList {};

// The internal types we want to support in the Column type
using AllValueTypes = TypeList<
    NodeID,
    EdgeID,
    EntityID,
    LabelID,
    EdgeTypeID,
    PropertyTypeID,
    LabelSetID,
    ValueType,
    types::Int64::Primitive,
    types::UInt64::Primitive,
    types::Double::Primitive,
    types::String::Primitive,
    types::Bool::Primitive,
    std::optional<types::Int64::Primitive>,
    std::optional<types::UInt64::Primitive>,
    std::optional<types::Double::Primitive>,
    std::optional<types::String::Primitive>,
    std::optional<types::Bool::Primitive>,
    std::string,
    const CommitBuilder*,
    const Change*
>;
template <ColumnKind::Code K, typename... TypeList>
struct ColumnTypeFromKindImpl;

// Attempt to match the provided ColumnKind Code with each internal and external type
template <ColumnKind::Code K, typename... Types>
struct ColumnTypeFromKindImpl<K, TypeList<Types...>> {
private:
    template <typename T>
    static consteval auto tryType() {
        if constexpr (K == ColumnVector<T>::staticKind()) {
            // Found the type: it's a ColumnVector
            return std::type_identity<ColumnVector<T>> {};
        } else if constexpr (K == ColumnConst<T>::staticKind()) {
            // Found the type: it's a ColumnConst
            return std::type_identity<ColumnConst<T>> {};
        } else {
            return std::type_identity<void> {}; // Type did not match
        }
    }

    // Head::Tail loop through all possible internal types
    template <typename First, typename... Rest>
    static consteval auto findMatch() {
        using Candidate = typename decltype(tryType<First>())::type; // Type we are looking for
        if constexpr (!std::is_void_v<Candidate>) {
            return std::type_identity<Candidate>{}; // Not void => found
        } else if constexpr (sizeof...(Rest) > 0) { // Other types left to check
            return findMatch<Rest...>();
        } else {
            return std::type_identity<void>{}; // No more types to check, not found: void
        }
    }

public:
    using type = typename decltype(findMatch<Types...>())::type;
};

};

// Get the ColumnT<U> type for a given ColumnKind code K
template <ColumnKind::Code K>
struct ColumnTypeFromKind {
    using type = typename ColumnTypeFromKindImpl<K, AllValueTypes>::type;
};

#define CASE_COMPONENT(col, Type)                                                        \
    return f(static_cast<ColumnTypeFromKind<Type::staticKind()>::type*>(col));

#define CONST_CASE_COMPONENT(col, Type)                                                  \
    return f(static_cast<const ColumnTypeFromKind<Type::staticKind()>::type*>(col));

#define COL_CASE(ColumnType)                                                             \
    case ColumnType::staticKind(): {                                                     \
        CASE_COMPONENT(col, ColumnType)                                                  \
    } break;

#define CONST_COL_CASE(ColumnType)                                                       \
    case ColumnType::staticKind(): {                                                     \
        CONST_CASE_COMPONENT(col, ColumnType)                                            \
    } break;

#define COLUMN_VECTOR_SWITCH(col)                                                        \
    switch ((col)->getKind()) {                                                          \
        COL_CASE(ColumnVector<NodeID>)                                                   \
        COL_CASE(ColumnVector<EdgeID>)                                                   \
        COL_CASE(ColumnVector<EntityID>)                                                 \
        COL_CASE(ColumnVector<LabelID>)                                                  \
        COL_CASE(ColumnVector<EdgeTypeID>)                                               \
        COL_CASE(ColumnVector<PropertyTypeID>)                                           \
        COL_CASE(ColumnVector<LabelSetID>)                                               \
        COL_CASE(ColumnVector<ValueType>)                                                \
        COL_CASE(ColumnVector<types::UInt64::Primitive>)                                 \
        COL_CASE(ColumnVector<types::Int64::Primitive>)                                  \
        COL_CASE(ColumnVector<types::Double::Primitive>)                                 \
        COL_CASE(ColumnVector<types::String::Primitive>)                                 \
        COL_CASE(ColumnVector<types::Bool::Primitive>)                                   \
        COL_CASE(ColumnOptVector<types::UInt64::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::Int64::Primitive>)                               \
        COL_CASE(ColumnOptVector<types::Double::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::String::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::Bool::Primitive>)                                \
        COL_CASE(ColumnVector<std::string>)                                              \
        COL_CASE(ColumnVector<const CommitBuilder*>)                                     \
        COL_CASE(ColumnVector<const Change*>)                                            \
                                                                                         \
        default: {                                                                       \
            throw FatalException(fmt::format(                                            \
                "Can not check result for column of kind {}", (col)->getKind()));        \
        }                                                                                \
    }

#define CONST_COLUMN_VECTOR_SWITCH(col)                                                  \
    switch ((col)->getKind()) {                                                          \
        CONST_COL_CASE(ColumnVector<NodeID>)                                             \
        CONST_COL_CASE(ColumnVector<EdgeID>)                                             \
        CONST_COL_CASE(ColumnVector<EntityID>)                                           \
        CONST_COL_CASE(ColumnVector<LabelID>)                                            \
        CONST_COL_CASE(ColumnVector<EdgeTypeID>)                                         \
        CONST_COL_CASE(ColumnVector<PropertyTypeID>)                                     \
        CONST_COL_CASE(ColumnVector<LabelSetID>)                                         \
        CONST_COL_CASE(ColumnVector<ValueType>)                                          \
        CONST_COL_CASE(ColumnVector<types::UInt64::Primitive>)                           \
        CONST_COL_CASE(ColumnVector<types::Int64::Primitive>)                            \
        CONST_COL_CASE(ColumnVector<types::Double::Primitive>)                           \
        CONST_COL_CASE(ColumnVector<types::String::Primitive>)                           \
        CONST_COL_CASE(ColumnVector<types::Bool::Primitive>)                             \
        CONST_COL_CASE(ColumnOptVector<types::UInt64::Primitive>)                        \
        CONST_COL_CASE(ColumnOptVector<types::Int64::Primitive>)                         \
        CONST_COL_CASE(ColumnOptVector<types::Double::Primitive>)                        \
        CONST_COL_CASE(ColumnOptVector<types::String::Primitive>)                        \
        CONST_COL_CASE(ColumnOptVector<types::Bool::Primitive>)                          \
        CONST_COL_CASE(ColumnVector<std::string>)                                        \
        CONST_COL_CASE(ColumnVector<const CommitBuilder*>)                               \
        CONST_COL_CASE(ColumnVector<const Change*>)                                      \
                                                                                         \
        default: {                                                                       \
            throw FatalException(fmt::format(                                            \
                "Can not check result for column of kind {}", (col)->getKind()));        \
        }                                                                                \
    }

template <typename F>
inline decltype(auto) dispatchColumnVector(Column* col, const F& f) {
    COLUMN_VECTOR_SWITCH(col);
}

template <typename F>
inline decltype(auto) dispatchColumnVector(const Column* col, const F& f) {
    CONST_COLUMN_VECTOR_SWITCH(col);
}

}
