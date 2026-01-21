#include "import/ImportResult.h"

#include <spdlog/fmt/bundled/format.h>

namespace vec {

std::string ImportError::fmtMessage() const {
    const std::string_view desc = ImportErrorTypeDescription::value(_type);
    return !_message.empty() 
        ? fmt::format("Vector import error: {}\n  -> {}", desc, _message)
        : fmt::format("Vector import error: {}", desc);
}

}
