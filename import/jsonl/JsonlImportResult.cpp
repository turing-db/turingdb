#include "JsonlImportResult.h"

#include <spdlog/fmt/bundled/format.h>

namespace db {

std::string JsonlImportError::fmtMessage() const {
    return _message.empty()
             ? fmt::format("JSONL error '{}' at line {}",
                           JsonlImportErrorTypeDescription::value(_type),
                           _line)
             : fmt::format("JSONL error '{}' at line {}:\n{}",
                           JsonlImportErrorTypeDescription::value(_type),
                           _line,
                           _message);
}

}
