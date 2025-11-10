#pragma once

#include "Column.h"
#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "ColumnConst.h"

#include "FatalException.h"

namespace db {

#define COL_CASE(ColumnType)                                                             \
    case ColumnType::staticKind(): {                                                     \
        CASE_COMPONENT(col, ColumnType)                                                  \
    } break;

#define COLUMN_SWITCH(col)                                                               \
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
        COL_CASE(ColumnOptVector<types::UInt64::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::Int64::Primitive>)                               \
        COL_CASE(ColumnOptVector<types::Double::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::String::Primitive>)                              \
        COL_CASE(ColumnOptVector<types::Bool::Primitive>)                                \
        COL_CASE(ColumnVector<std::string>)                                              \
        COL_CASE(ColumnConst<EntityID>)                                                  \
        COL_CASE(ColumnConst<NodeID>)                                                    \
        COL_CASE(ColumnConst<EdgeID>)                                                    \
        COL_CASE(ColumnConst<types::UInt64::Primitive>)                                  \
        COL_CASE(ColumnConst<types::Int64::Primitive>)                                   \
        COL_CASE(ColumnConst<types::Double::Primitive>)                                  \
        COL_CASE(ColumnConst<types::String::Primitive>)                                  \
        COL_CASE(ColumnConst<types::Bool::Primitive>)                                    \
        COL_CASE(ColumnVector<const CommitBuilder*>)                                     \
        COL_CASE(ColumnVector<const Change*>)                                            \
                                                                                         \
        default: {                                                                       \
            throw FatalException(fmt::format(                                            \
                "Can not check result for column of kind {}", (col)->getKind()));        \
        }

}
