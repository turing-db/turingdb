#include "DataPartMergeResult.h"

using namespace db;

std::string DataPartMergeError::fmtMessage() const {
    return std::string {DataPartMergeErrorTypeDescription::value(_type)};
}
