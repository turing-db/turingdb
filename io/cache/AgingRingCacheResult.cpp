#include "AgingRingCacheResult.h"

#include <spdlog/fmt/bundled/format.h>

std::string AgingRingCacheError::fmtMessage() const {
    const std::string_view desc = AgingRingCacheErrorTypeDescription::value(_type);

    return fmt::format("Cache error: {}", desc);
}
