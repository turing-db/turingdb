#pragma once

#include <vector>
#include <string>
#include <string_view>

enum CMD {
    INSERT,
    PRINT,
    REMOVE
};

std::vector<std::string> split(std::string_view str, std::string_view delim);

std::string alphaNumericise(const std::string_view in);

std::vector<std::string> preprocess(const std::string_view in);

