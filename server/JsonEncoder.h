#pragma once

#include "PayloadWriter.h"
#include "metadata/PropertyType.h"
#include "ID.h"
#include "columns/Block.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "dataframe/Dataframe.h"

namespace db {

#define JSON_ENCODE_COL_CASE(Type)                        \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        writer.arr();                                     \
        if (!src.empty()) {                               \
            auto it = src.begin();                        \
            writer.value(*it);                            \
            ++it;                                         \
            for (; it != src.end(); ++it) {               \
                writer.value(*it);                        \
            }                                             \
        }                                                 \
        writer.end();                                     \
        return;                                           \
    }

#define JSON_ENCODE_COL_CONST_CASE(Type)                \
    case Type::staticKind(): {                          \
        const Type& c = *static_cast<const Type*>(col); \
        writer.value(c.getRaw());                       \
        return;                                         \
    }

#define JSON_COLUMN_TYPE(Type, TypeName)     \
    case ColumnVector<Type>::staticKind(): { \
        writer.value(TypeName);              \
    } break;                                 \
    case ColumnOptVector<Type>::staticKind(): { \
        writer.value(TypeName);                 \
    } break;                                    \
    case ColumnConst<Type>::staticKind(): {  \
        writer.value(TypeName);              \
    } break;                                 \


class JsonEncoder {
public:
    JsonEncoder() = delete;

    static void writeDataframeHeader(PayloadWriter& writer, const Dataframe& df) {
        writer.key("header");
        writer.obj();
        writer.key("column_names");
        writer.arr();

        for (const NamedColumn* namedCol : df.cols()) {
            const std::string_view name = namedCol->getName();
            if (name.empty()) {
                const ColumnTag tag = namedCol->getTag();
                writer.value("$" + std::to_string(tag.getValue()));
            } else {
                writer.value(name);
            }
        }

        writer.end();

        writer.key("column_types");
        writer.arr();

        for (const NamedColumn* namedCol : df.cols()) {
            const Column* col = namedCol->getColumn();
            switch (col->getKind()) {
                JSON_COLUMN_TYPE(EntityID, "UInt64")
                JSON_COLUMN_TYPE(types::UInt64::Primitive, "Uint64")
                JSON_COLUMN_TYPE(types::Int64::Primitive, "Int64")
                JSON_COLUMN_TYPE(types::Double::Primitive, "Double")
                JSON_COLUMN_TYPE(types::Bool::Primitive, "Bool")
                JSON_COLUMN_TYPE(types::String::Primitive, "String")
                default: {
                    bioassert(false, "Unexpected ColumnKind {}", col->getKind());
                } break;
            }
        }

        writer.end();

        writer.end();
    }

    static void writeDataframe(PayloadWriter& writer, const Dataframe& df) {
        writer.arr();
        for (const NamedColumn* namedCol : df.cols()) {
            writeColumn(writer, namedCol->getColumn());
        }

        writer.end();
    }

    static void writeColumn(PayloadWriter& writer, const Column* col) {
        switch (col->getKind()) {
            JSON_ENCODE_COL_CASE(ColumnVector<EntityID>)
            JSON_ENCODE_COL_CASE(ColumnVector<NodeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<EdgeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<LabelID>)
            JSON_ENCODE_COL_CASE(ColumnVector<EdgeTypeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<PropertyTypeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<LabelSetID>)
            JSON_ENCODE_COL_CASE(ColumnVector<ChangeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<types::UInt64::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnVector<types::Int64::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnVector<types::Double::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnVector<types::String::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnVector<types::Bool::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnOptVector<types::UInt64::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnOptVector<types::Int64::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnOptVector<types::Double::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnOptVector<types::String::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnOptVector<types::Bool::Primitive>)
            JSON_ENCODE_COL_CASE(ColumnVector<ValueType>);
            JSON_ENCODE_COL_CASE(ColumnVector<std::string>)
            JSON_ENCODE_COL_CASE(ColumnVector<const CommitBuilder*>)
            JSON_ENCODE_COL_CASE(ColumnVector<const Change*>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<EntityID>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<NodeID>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<EdgeID>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<types::UInt64::Primitive>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<types::Int64::Primitive>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<types::Double::Primitive>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<types::String::Primitive>)
            JSON_ENCODE_COL_CONST_CASE(ColumnConst<types::Bool::Primitive>)

            default: {
                panic("writeColumn not handled for columns of kind {}", col->getKind());
            }
        }
    }
};

}
