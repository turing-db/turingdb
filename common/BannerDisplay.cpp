#include "BannerDisplay.h"

#include <string>
#include <sstream>
#include <iostream>

void BannerDisplay::getBannerString(std::string& result) {
    std::ostringstream banner;
    banner << "******* Turing Platform\n";
    banner << "    *** Copyright Turing Biosystems Ltd. All Rights Reserved.\n";
    result = banner.str();
}

void BannerDisplay::printBanner() {
    std::string banner;
    getBannerString(banner);
    std::cout << banner << '\n';
}
