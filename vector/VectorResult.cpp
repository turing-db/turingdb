#include "VectorResult.h"

#include <spdlog/fmt/bundled/format.h>

namespace vec {

std::string VectorError::fmtMessage() const {
    const std::string_view desc = VectorErrorTypeDescription::value(_type);
    return _fileError.has_value() 
        ? fmt::format("Vector database error: {}\n  -> {}", desc, _fileError.value().fmtMessage())
        : fmt::format("Vector database error: {}", desc);
}

}
