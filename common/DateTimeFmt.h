#pragma once

#include <time.h>
#include <string>

#include "spdlog/fmt/bundled/format.h"

inline std::string formatUnixTime(long long seconds) {
    time_t t = static_cast<time_t>(seconds);
    tm* tm   = gmtime(&t); // or std::localtime(&t) for local time

    char buf[64];
    strftime(buf, sizeof(buf), "%d-%m-%Y %H:%M:%S", tm);

    return fmt::format("{}", buf);
}
