#include "VectorResult.h"

#include <spdlog/fmt/bundled/format.h>

namespace vec {

std::string VectorError::fmtMessage() const {
    return fmt::format("VectorDB error: {}", VectorErrorTypeDescription::value(_type));
}

}
