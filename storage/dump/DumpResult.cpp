#include "DumpResult.h"

#include <spdlog/fmt/bundled/core.h>

using namespace db;

std::string DumpError::fmtMessage() const {
    return !_fileError.has_value()
             ? fmt::format("Graph dump/load error: {}",
                           DumpErrorTypeDescription::value(_type))
             : fmt::format("Graph dump/load error: {} ([details {}])",
                           DumpErrorTypeDescription::value(_type),
                           _fileError.value().fmtMessage());
}
