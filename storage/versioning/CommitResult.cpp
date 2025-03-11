#include "CommitResult.h"

namespace db {

std::string CommitError::fmtMessage() const {
    return fmt::format("Version controller error: {}",
                       CommitErrorTypeDescription::value(_type));
}

}
