#pragma once

#include <string_view>
#include <vector>

class StringUtils {
public:
    StringUtils() = delete;

    static void splitString(std::string_view str,
                            char sep,
                            std::vector<std::string_view>& res);
};
