#pragma once

#include <stddef.h>
#include <array>
#include <string>

class BannerDisplay {
public:
    static constexpr size_t LOGO_LINE_COUNT = 6;

    // The TuringDB block-font logo, one entry per row (no trailing newline). The
    // single source of truth for the logo; callers that need the banner or the
    // forge animation share these lines.
    static const std::array<const char*, LOGO_LINE_COUNT>& getLogoLines();

    static void getBannerString(std::string& result);

    static void printBanner();
};
