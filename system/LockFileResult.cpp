#include "LockFileResult.h"

namespace db {

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
