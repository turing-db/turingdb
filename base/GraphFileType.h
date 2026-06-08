#pragma once

#include "EnumToString.h"

namespace db {

enum class GraphFileType {
    UNKNOWN,
    GML,
    JSONL,
    BINARY,
    _SIZE
};

using GraphFileTypeDescription = EnumToString<GraphFileType>::Create<
    EnumStringPair<GraphFileType::UNKNOWN, "UNKNOWN">,
    EnumStringPair<GraphFileType::GML, "GML">,
    EnumStringPair<GraphFileType::JSONL, "JSONL">,
    EnumStringPair<GraphFileType::BINARY, "BINARY">>;
}
