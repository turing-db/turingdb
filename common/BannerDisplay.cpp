#include "BannerDisplay.h"

#include <stddef.h>
#include <array>
#include <string>
#include <sstream>
#include <iostream>

const std::array<const char*, BannerDisplay::LOGO_LINE_COUNT>& BannerDisplay::getLogoLines() {
    static const std::array<const char*, LOGO_LINE_COUNT> logoLines {{
        "████████╗██╗   ██╗██████╗ ██╗███╗   ██╗ ██████╗     ██████╗ ██████╗ ",
        "╚══██╔══╝██║   ██║██╔══██╗██║████╗  ██║██╔════╝     ██╔══██╗██╔══██╗",
        "   ██║   ██║   ██║██████╔╝██║██╔██╗ ██║██║  ███╗    ██║  ██║██████╔╝",
        "   ██║   ██║   ██║██╔══██╗██║██║╚██╗██║██║   ██║    ██║  ██║██╔══██╗",
        "   ██║   ╚██████╔╝██║  ██║██║██║ ╚████║╚██████╔╝    ██████╔╝██████╔╝",
        "   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═════╝ ╚═════╝ ",
    }};
    return logoLines;
}

void BannerDisplay::getBannerString(std::string& result) {
    std::ostringstream banner;
    banner << "******* Turing DB\n";
    banner << "    *** Copyright Turing Biosystems Ltd. All Rights Reserved.\n\n";

    for (const char* line : getLogoLines()) {
        banner << line << '\n';
    }

    result = banner.str();
}

void BannerDisplay::printBanner() {
    std::string banner;
    getBannerString(banner);
    std::cout << banner << '\n';
}
