#include "BannerDisplay.h"

#include <string>
#include <sstream>
#include <iostream>

void BannerDisplay::getBannerString(std::string& result) {
    std::ostringstream banner;
    banner << "******* Turing DB\n";
    banner << "    *** Copyright Turing Biosystems Ltd. All Rights Reserved.\n\n";
    banner << "████████╗██╗   ██╗██████╗ ██╗███╗   ██╗ ██████╗     ██████╗ ██████╗ \n"
              "╚══██╔══╝██║   ██║██╔══██╗██║████╗  ██║██╔════╝     ██╔══██╗██╔══██╗\n"
              "   ██║   ██║   ██║██████╔╝██║██╔██╗ ██║██║  ███╗    ██║  ██║██████╔╝\n"
              "   ██║   ██║   ██║██╔══██╗██║██║╚██╗██║██║   ██║    ██║  ██║██╔══██╗\n"
              "   ██║   ╚██████╔╝██║  ██║██║██║ ╚████║╚██████╔╝    ██████╔╝██████╔╝\n"
              "   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═════╝ ╚═════╝ \n";

    result = banner.str();
}

void BannerDisplay::printBanner() {
    std::string banner;
    getBannerString(banner);
    std::cout << banner << '\n';
}
