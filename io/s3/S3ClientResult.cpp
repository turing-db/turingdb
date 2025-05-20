#include "S3ClientResult.h"

using namespace S3;

std::string S3ClientError::fmtMessage() const {
    return fmt::format("Turing S3Client error: {}",
                       ErrorTypeDescription::value(_type));
}
