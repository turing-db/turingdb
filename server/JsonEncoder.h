#pragma once

#include "PayloadWriter.h"
#include "columns/Block.h"
#include "columns/Column.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOptVector.h"
#include "types/PropertyType.h"
#include "EntityID.h"

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
