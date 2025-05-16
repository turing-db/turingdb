#include "FileCacheResult.h"

namespace db {

std::string Error::fmtMessage() const {
    return fmt::format("Turing S3Client error: {}",
                       ErrorTypeDescription::value(_type));
}

}
