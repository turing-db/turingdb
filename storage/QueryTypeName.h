#pragma once

#include <string_view>

#include "CellTag.h"
#include "metadata/PropertyType.h"

namespace db {

// The name of the type of a value a row holds, spelled as the analyzer's EvaluatedTypeName
// spells it, so a runtime type error names a type as its analysis-time twin does
std::string_view queryTypeName(CellTag tag);
std::string_view queryTypeName(ValueType type);

}
