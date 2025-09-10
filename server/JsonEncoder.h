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
#include "ChangeCommand.h"
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
        writer.key("header");
        writer.obj();
        writer.key("column_names");
        writer.arr();

        switch (cmd->getKind()) {
            case QueryCommand::Kind::MATCH_COMMAND: {
                const auto& fields = static_cast<const MatchCommand*>(cmd)->getProjection()->returnFields();

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
            }
            break;

            case QueryCommand::Kind::EXPLAIN_COMMAND: {
                writer.value("Plan Explanation");
                writer.end();

                writer.key("column_types");
                writer.arr();
                writer.value(ValueTypeName::value(ValueType::String));
            }
            break;

            case QueryCommand::Kind::HISTORY_COMMAND: {
                // JSON doesn't support the single escaped
                // newline character
                writer.end();

                writer.key("column_types");
                writer.arr();
            }
            break;

            case QueryCommand::Kind::CHANGE_COMMAND: {
                const auto& changeCmd = static_cast<const ChangeCommand*>(cmd);
                const auto changeOp = changeCmd->getChangeOpType();
                if (changeOp == ChangeOpType::NEW) {
                    writer.value("Change ID");
                    writer.end();

                    writer.key("column_types");
                    writer.arr();
                    writer.value(ValueTypeName::value(ValueType::UInt64));
                } else {
                    writer.end();
                    writer.key("column_types");
                    writer.arr();
                }
            }
            break;

            case QueryCommand::Kind::CALL_COMMAND: {
                const auto& callCmd = static_cast<const CallCommand*>(cmd);

                for (const auto& name : callCmd->getColNames()) {
                    writer.value(name);
                }
                writer.end();

                writer.key("column_types");
                writer.arr();

                for (const auto& type : callCmd->getColTypes()) {
                    writer.value(ValueTypeName::value(type));
                }
            }
            break;

            case QueryCommand::Kind::LIST_GRAPH_COMMAND: {
                writer.value("graph");
                writer.end();

                writer.key("column_types");
                writer.arr();
                writer.value(ValueTypeName::value(ValueType::String));
            }
            break;

            case QueryCommand::Kind::CREATE_COMMAND:
            case QueryCommand::Kind::COMMIT_COMMAND:
            case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            case QueryCommand::Kind::S3CONNECT_COMMAND:
            case QueryCommand::Kind::S3TRANSFER_COMMAND:
            case QueryCommand::Kind::LOAD_GRAPH_COMMAND: {
                writer.end();
                writer.key("column_types");
                writer.arr();
            }
            break;
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
            JSON_ENCODE_COL_CASE(ColumnVector<const Change*>)
            JSON_ENCODE_COL_CASE(ColumnVector<std::string>)
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
