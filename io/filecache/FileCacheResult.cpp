#include "FileCacheResult.h"

using namespace db;

std::string FileCacheError::fmtMessage() const {
    return fmt::format("Turing S3Client error: {}",
                       ErrorTypeDescription::value(_type));
}
