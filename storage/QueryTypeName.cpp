#include "QueryTypeName.h"

#include "FatalException.h"

using namespace db;

std::string_view db::queryTypeName(CellTag tag) {
    switch (tag) {
        case CellTag::Int:
        case CellTag::UInt:
            return "Integer";
        break;
        case CellTag::Double:
            return "Double";
        break;
        case CellTag::Bool:
            return "Bool";
        break;
        case CellTag::String:
            return "String";
        break;
        case CellTag::Embedding:
            return "Embedding";
        break;
        case CellTag::ListView:
            return "List";
        break;
        case CellTag::Null:
            return "Null";
        break;
        case CellTag::NodeID:
            return "NodePattern";
        break;
        case CellTag::EdgeID:
            return "EdgePattern";
        break;
        case CellTag::DateTime:
            return "DateTime";
        break;
        case CellTag::MapView:
            return "Map";
        break;
        case CellTag::Duration:
            return "Duration";
        break;
        case CellTag::Path:
            return "GraphPath";
        break;
        case CellTag::Point:
            return "Point";
        break;
        case CellTag::INVALID:
            return "Invalid";
        break;
    }

    throw FatalException("Unknown CellTag");
}

std::string_view db::queryTypeName(ValueType type) {
    switch (type) {
        case ValueType::Int64:
        case ValueType::UInt64:
            return "Integer";
        break;
        case ValueType::Double:
            return "Double";
        break;
        case ValueType::String:
            return "String";
        break;
        case ValueType::Bool:
            return "Bool";
        break;
        case ValueType::Embedding:
            return "Embedding";
        break;
        case ValueType::List:
            return "List";
        break;
        case ValueType::DateTime:
            return "DateTime";
        break;
        case ValueType::Map:
            return "Map";
        break;
        case ValueType::Duration:
            return "Duration";
        break;
        case ValueType::Point:
            return "Point";
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            return "Invalid";
        break;
    }

    throw FatalException("Unknown ValueType");
}
