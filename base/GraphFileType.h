#pragma once

#include "EnumToString.h"

namespace db {

enum class GraphFileType {
    GML,
    NEO4J_JSON,
    NEO4J,
    BINARY,
    _SIZE
};

using GraphFileTypeDescription = EnumToString<GraphFileType>::Create<
    EnumStringPair<GraphFileType::GML, "GML">,
    EnumStringPair<GraphFileType::NEO4J_JSON, "NEO4J_JSON">,
    EnumStringPair<GraphFileType::NEO4J, "NEO4J">,
    EnumStringPair<GraphFileType::BINARY, "BINARY">>;
}
