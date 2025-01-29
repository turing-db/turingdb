#pragma once

#include <string>
#include <string_view>
#include <optional>

#include "Block.h"
#include "ColumnVector.h"
#include "ColumnOptVector.h"
#include "EntityID.h"
#include "PropertyType.h"

#include "Panic.h"

namespace db {

#define JSON_ENCODE_COL_CASE(Type)                        \
    case Type::staticKind(): {                            \
        const Type& src = *static_cast<const Type*>(col); \
        writer->write('[');                               \
        if (!src.empty()) {                               \
            auto it = src.begin();                        \
            writeData(writer, *it);                       \
            ++it;                                         \
            for (; it != src.end(); ++it) {               \
                writer->write(',');                       \
                writeData(writer, *it);                   \
            }                                             \
        }                                                 \
        writer->write(']');                               \
        return;                                           \
    }

template <typename Writer>
requires
requires(Writer writer,
         char c,
         const std::string& str,
         std::string_view strView) {
    writer.write(c);
    writer.write(str);
    writer.write(strView);
}
class JsonEncodingUtils {
public:
    inline static void writeBlock(const Block* block, Writer* writer) {
        const auto& columns = block->columns();
        writer->write('[');

        if (!columns.empty()) {
            auto it = columns.begin();
            writeColumn(*it, writer);
            ++it;

            for (; it != columns.end(); ++it) {
                writer->write(',');
                writeColumn(*it, writer);
            }
        }

        writer->write(']');
    }

    inline static void writeColumn(const Column* col, Writer* writer) {
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

private:
    JsonEncodingUtils() = delete;

    inline static void writeData(Writer* writer, EntityID elem) {
        writeData(writer, elem.getValue());
    }

    inline static void writeData(Writer* writer, const std::string& elem) {
        writer->write('"');
        writer->write(elem);
        writer->write('"');
    }

    inline static void writeData(Writer* writer, const std::string_view& elem) {
        writer->write('"');
        writer->write(elem);
        writer->write('"');
    }

    inline static void writeData(Writer* writer, CustomBool elem) {
        writer->write(elem._boolean ? "true" : "false");
    }

    inline static void writeData(Writer* writer, std::unsigned_integral auto elem) {
        writer->write(std::to_string(elem));
    }

    inline static void writeData(Writer* writer, std::signed_integral auto elem) {
        writer->write(std::to_string(elem));
    }

    inline static void writeData(Writer* writer, std::floating_point auto elem) {
        writer->write(std::to_string(elem));
    }

    template <typename T>
    inline static void writeData(Writer* writer, const std::optional<T>& elem) {
        if (elem) {
            writeData(writer, *elem);
        } else {
            writer->write("null");
        }
    }
};

}
