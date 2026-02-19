#include "LockFileResult.h"

namespace db {

LockFileError::LockFileError(LockFileErrorType type,
                             const std::string& msg)
    : _type(type),
    _message(msg)
{
}

std::string LockFileError::fmtMessage() const {
    if (_message.empty()) {
        return fmt::format("Lock file error: {}",
                           LockFileErrorTypeDescription::value(_type));
    } else {
        return fmt::format("Lock file error: {} ({})",
                           LockFileErrorTypeDescription::value(_type),
                           _message);
    }
}

}
