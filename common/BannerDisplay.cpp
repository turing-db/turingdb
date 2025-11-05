#include "BannerDisplay.h"

#include <string>
#include <sstream>
#include <iostream>

std::string BannerDisplay::getBannerString() {
    std::ostringstream banner;
    banner << "******* Turing Platform\n";
    banner << "    *** Copyright Turing Biosystems Ltd. All Rights Reserved.\n";
    return banner.str();
}

void BannerDisplay::printBanner() {
    std::cout << getBannerString() << '\n';
}
