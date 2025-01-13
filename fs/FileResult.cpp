#include "FileResult.h"

namespace fs {

std::string Error::fmtMessage() const {
    return _errno == -1
             ? fmt::format("Filesystem error: {}",
                           ErrorTypeDescription::value(_type))
             : fmt::format("Filesystem error: {} ({})",
                           ErrorTypeDescription::value(_type),
                           strerror(_errno));
}

}
