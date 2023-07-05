// Copyright 2023 Turing Biosystems Ltd.

#include "ValueType.h"

using namespace db;

ValueType::ValueType(ValueKind kind)
    : _kind(kind)
{
}

std::string ValueType::toString() const {
    switch (_kind) {
        case VK_INVALID:
            return "invalid";
        case VK_INT:
            return "int";

        case VK_UNSIGNED:
            return "unsigned";

        case VK_BOOL:
            return "bool";

        case VK_DECIMAL:
            return "decimal";

        case VK_STRING_REF:
            return "StringRef";

        case VK_STRING:
            return "string";

        default:
            return "UNKNOWN";
    }
}
