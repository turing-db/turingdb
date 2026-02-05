#pragma once

#include <sstream>

template <typename NumberType>
NumberType StringToNumber(std::string_view str, bool& error) {
    try {
        NumberType res {0};
        std::istringstream iss;
        iss >> res;
        error = false;
        return res;
    } catch (const std::exception& e) {
        error = true;
        return 0;
    }
}
