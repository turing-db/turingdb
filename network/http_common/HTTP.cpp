#include "HTTP.h"

using namespace net;

HTTP::Status HTTP::codeToStatus(size_t httpCode) {
    const auto findIt = std::find(STATUS_CODES.begin(),
                                  STATUS_CODES.end(),
                                  httpCode);
    if (findIt == STATUS_CODES.end()) {
        return Status::BAD_REQUEST;
    }

    const size_t pos = findIt-STATUS_CODES.begin();
    return (Status)((size_t)Status::OK+pos);
}