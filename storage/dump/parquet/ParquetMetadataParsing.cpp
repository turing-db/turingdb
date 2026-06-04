#include "ParquetMetadataParsing.h"

#include <charconv>

#include <spdlog/fmt/fmt.h>

#include "FatalException.h"

using namespace db;

uint64_t db::parseMetadataUint64(const std::string& key, const std::string& value) {
    uint64_t result = 0;
    const char* end = value.data() + value.size();
    const auto parseResult = std::from_chars(value.data(), end, result);

    const bool parsedFully = parseResult.ec == std::errc() && parseResult.ptr == end;
    if (!parsedFully) {
        throw FatalException(fmt::format(
            "Parquet metadata key '{}' has invalid unsigned integer value '{}'", key, value));
    }

    return result;
}
