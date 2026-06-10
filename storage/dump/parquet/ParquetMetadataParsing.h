#pragma once

#include <stdint.h>

#include <string>

namespace db {

// Parses an unsigned decimal scalar out of Parquet key/value metadata. The dumpers only
// write values produced by std::to_string, so anything that does not parse back as a
// full unsigned integer is a corrupt dump: throws FatalException instead of leaking the
// standard parsers' std::invalid_argument / std::out_of_range.
uint64_t parseMetadataUint64(const std::string& key, const std::string& value);

}
