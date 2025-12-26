#pragma once

#include "QueryCommand.h"
#include "PayloadWriter.h"
#include "ReturnField.h"
#include "ReturnProjection.h"
#include "metadata/PropertyType.h"
#include "ChangeCommand.h"
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
            } break;

            case QueryCommand::Kind::EXPLAIN_COMMAND: {
                writer.value("Plan Explanation");
                writer.end();

                writer.key("column_types");
                writer.arr();
                writer.value(ValueTypeName::value(ValueType::String));
            } break;

            case QueryCommand::Kind::HISTORY_COMMAND: {
                // JSON doesn't support the single escaped
                // newline character
                writer.end();

                writer.key("column_types");
                writer.arr();
            } break;

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
            } break;

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
            } break;

            case QueryCommand::Kind::LIST_GRAPH_COMMAND: {
                writer.value("graph");
                writer.end();

                writer.key("column_types");
                writer.arr();
                writer.value(ValueTypeName::value(ValueType::String));
            } break;

            case QueryCommand::Kind::CREATE_COMMAND:
            case QueryCommand::Kind::DELETE_NODES_COMMAND:
            case QueryCommand::Kind::DELETE_EDGES_COMMAND:
            case QueryCommand::Kind::COMMIT_COMMAND:
            case QueryCommand::Kind::DATAPARTMERGE_COMMAND:
            case QueryCommand::Kind::CREATE_GRAPH_COMMAND:
            case QueryCommand::Kind::S3CONNECT_COMMAND:
            case QueryCommand::Kind::S3TRANSFER_COMMAND:
            case QueryCommand::Kind::IMPORT_GRAPH_COMMAND:
            case QueryCommand::Kind::LOAD_GRAPH_COMMAND: {
                writer.end();
                writer.key("column_types");
                writer.arr();
            } break;
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
                case ColumnVector<EntityID>::staticKind():
                case ColumnVector<NodeID>::staticKind():
                case ColumnVector<EdgeID>::staticKind():
                case ColumnVector<PropertyTypeID>::staticKind():
                case ColumnVector<LabelID>::staticKind():
                case ColumnVector<EdgeTypeID>::staticKind():
                case ColumnVector<LabelSetID>::staticKind():
                case ColumnVector<types::UInt64::Primitive>::staticKind(): {
                    writer.value("UInt64");
                } break;
                case ColumnVector<types::Int64::Primitive>::staticKind(): {
                    writer.value("Int64");
                } break;
                case ColumnVector<types::Double::Primitive>::staticKind(): {
                    writer.value("Double");
                } break;
                case ColumnVector<types::Bool::Primitive>::staticKind(): {
                    writer.value("Bool");
                } break;
                case ColumnVector<types::String::Primitive>::staticKind():
                case ColumnVector<std::string>::staticKind():
                case ColumnVector<ValueType>::staticKind():
                case ColumnVector<ChangeID>::staticKind(): {
                    writer.value("String");
                } break;
                case ColumnOptVector<types::UInt64::Primitive>::staticKind(): {
                    writer.value("UInt64");
                } break;
                case ColumnOptVector<types::Int64::Primitive>::staticKind(): {
                    writer.value("Int64");
                } break;
                case ColumnOptVector<types::Double::Primitive>::staticKind(): {
                    writer.value("Double");
                } break;
                case ColumnOptVector<types::String::Primitive>::staticKind(): {
                    writer.value("String");
                } break;
                case ColumnOptVector<types::Bool::Primitive>::staticKind(): {
                    writer.value("Bool");
                } break;
                default: {
                    writer.value("String");
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
