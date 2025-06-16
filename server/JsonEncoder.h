#pragma once

#include "QueryCommand.h"
#include "PayloadWriter.h"
#include "ReturnField.h"
#include "ReturnProjection.h"
#include "columns/Block.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOptVector.h"
#include "metadata/PropertyType.h"
#include "ID.h"

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

class JsonEncoder {
public:
    JsonEncoder() = delete;

    static void writeHeader(PayloadWriter& writer, const QueryCommand* cmd) {
        const auto& fields = static_cast<const MatchCommand*>(cmd)->getProjection()->returnFields();

        writer.key("header");
        writer.obj();
        writer.key("column_names");
        writer.arr();

        for (const auto& field : fields) {
            if (field->getMemberType().isValid()) {
                writer.value(field->getName() + "." + field->getMemberName());
            } else {
                writer.value(field->getName() + field->getMemberName());
            }
        }
        writer.end();

        writer.key("column_types");
        writer.arr();

        for (const auto& field : fields) {
            if (field->getMemberType().isValid()) {
                writer.value(ValueTypeName::value(field->getMemberType()._valueType));
            } else {
                writer.value(ValueTypeName::value(ValueType::UInt64));
            }
        }
        writer.end();
        writer.end();

        writer.key("data");
        writer.arr();
    }

    static void writeBlock(PayloadWriter& writer, const Block& block) {
        writer.arr();
        for (const Column* col : block.columns()) {
            writeColumn(writer, col);
        }
        writer.end();
    }

    static void writeColumn(PayloadWriter& writer, const Column* col) {
        switch (col->getKind()) {
            JSON_ENCODE_COL_CASE(ColumnVector<EntityID>)
            JSON_ENCODE_COL_CASE(ColumnVector<NodeID>)
            JSON_ENCODE_COL_CASE(ColumnVector<EdgeID>)
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

            default: {
                panic("writeColumn not handled for columns of kind {}", col->getKind());
            }
        }
    }
};

}
