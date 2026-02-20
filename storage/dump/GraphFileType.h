#pragma once

#include <stdint.h>

#include "EnumToString.h"

namespace db {

enum class GraphFileType : uint8_t {
    GML = 0,
    BINARY,
    _SIZE
};

using GraphFileTypeDescription = EnumToString<GraphFileType>::Create<
    EnumStringPair<GraphFileType::GML, "GML">,
    EnumStringPair<GraphFileType::BINARY, "BINARY">
>;

}
