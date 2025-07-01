#pragma once

#include <vector>
#include <string>

enum CMD {
    INSERT,
    PRINT,
    REMOVE
};

std::vector<std::string> split(std::string_view str, std::string_view delim);

std::vector<std::string> preprocess(std::string);
