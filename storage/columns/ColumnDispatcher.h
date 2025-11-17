#pragma once

#include "Column.h"
#include "ColumnVector.h"
#include "ColumnConst.h"
#include "ColumnOptVector.h"
#include "ColumnKind.h"
#include "metadata/PropertyType.h"

#include "FatalException.h"

namespace db {

template <ColumnKind::ColumnKindCode K>
struct ColumnTypeFromKind;

template <>
struct ColumnTypeFromKind<ColumnVector<NodeID>::staticKind()> {
    using type = ColumnVector<NodeID>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<EdgeID>::staticKind()> {
    using type = ColumnVector<EdgeID>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<EntityID>::staticKind()> {
    using type = ColumnVector<EntityID>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<PropertyTypeID>::staticKind()> {
    using type = ColumnVector<PropertyTypeID>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<LabelSetID>::staticKind()> {
    using type = ColumnVector<LabelSetID>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<types::UInt64::Primitive>::staticKind()> {
    using type = ColumnVector<types::UInt64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<types::Int64::Primitive>::staticKind()> {
    using type = ColumnVector<types::Int64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<types::Double::Primitive>::staticKind()> {
    using type = ColumnVector<types::Double::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<types::String::Primitive>::staticKind()> {
    using type = ColumnVector<types::String::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<types::Bool::Primitive>::staticKind()> {
    using type = ColumnVector<types::Bool::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<std::string>::staticKind()> {
    using type = ColumnVector<std::string>;
};

template <>
struct ColumnTypeFromKind<ColumnOptVector<types::UInt64::Primitive>::staticKind()> {
    using type = ColumnOptVector<types::UInt64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnOptVector<types::Int64::Primitive>::staticKind()> {
    using type = ColumnOptVector<types::Int64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnOptVector<types::Double::Primitive>::staticKind()> {
    using type = ColumnOptVector<types::Double::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnOptVector<types::String::Primitive>::staticKind()> {
    using type = ColumnOptVector<types::String::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnOptVector<types::Bool::Primitive>::staticKind()> {
    using type = ColumnOptVector<types::Bool::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<NodeID>::staticKind()> {
    using type = ColumnConst<NodeID>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<EdgeID>::staticKind()> {
    using type = ColumnConst<EdgeID>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<EntityID>::staticKind()> {
    using type = ColumnConst<EntityID>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<types::UInt64::Primitive>::staticKind()> {
    using type = ColumnConst<types::UInt64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<types::Int64::Primitive>::staticKind()> {
    using type = ColumnConst<types::Int64::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<types::Double::Primitive>::staticKind()> {
    using type = ColumnConst<types::Double::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<types::String::Primitive>::staticKind()> {
    using type = ColumnConst<types::String::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnConst<types::Bool::Primitive>::staticKind()> {
    using type = ColumnConst<types::Bool::Primitive>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<const CommitBuilder*>::staticKind()> {
    using type = ColumnVector<const CommitBuilder*>;
};

template <>
struct ColumnTypeFromKind<ColumnVector<const Change*>::staticKind()> {
    using type = ColumnVector<const Change*>;
};

#define CASE_COMPONENT(col, Type)                                                        \
    return f(static_cast<ColumnTypeFromKind<Type::staticKind()>::type*>(col));

#define COL_CASE(ColumnType)                                                             \
    case ColumnType::staticKind(): {                                                     \
        CASE_COMPONENT(col, ColumnType)                                                  \
    } break;

#define COLUMN_VECTOR_SWITCH(col)                                                        \
    switch ((col)->getKind()) {                                                          \
        COL_CASE(ColumnVector<NodeID>)                                                   \
        COL_CASE(ColumnVector<EdgeID>)                                                   \
        COL_CASE(ColumnVector<EntityID>)                                                 \
        COL_CASE(ColumnVector<PropertyTypeID>)                                           \
        COL_CASE(ColumnVector<LabelSetID>)                                               \
        COL_CASE(ColumnVector<types::UInt64::Primitive>)                                 \
        COL_CASE(ColumnVector<types::Int64::Primitive>)                                  \
        COL_CASE(ColumnVector<types::Double::Primitive>)                                 \
        COL_CASE(ColumnVector<types::String::Primitive>)                                 \
        COL_CASE(ColumnVector<types::Bool::Primitive>)                                   \
        COL_CASE(ColumnVector<std::string>)                                              \
        COL_CASE(ColumnVector<const CommitBuilder*>)                                     \
        COL_CASE(ColumnVector<const Change*>)                                            \
                                                                                         \
        default: {                                                                       \
            throw FatalException(fmt::format(                                            \
                "Can not check result for column of kind {}", (col)->getKind()));        \
        }                                                                                \
    }



template <typename F>
inline decltype(auto) dispatchColumnVector(Column* col, F&& f) {
    COLUMN_VECTOR_SWITCH(col);
}

}
