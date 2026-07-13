#include "LoadGraphResult.h"

#include <spdlog/fmt/bundled/core.h>

using namespace db;

std::string LoadGraphError::fmtMessage() const {
    return !_dumpError.has_value()
             ? fmt::format("Graph load error: {}",
                           LoadGraphErrorTypeDescription::value(_type))
             : fmt::format("Graph load error: {} ([details {}])",
                           LoadGraphErrorTypeDescription::value(_type),
                           _dumpError.value().fmtMessage());
}
