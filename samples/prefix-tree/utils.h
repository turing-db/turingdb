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

bool replace(std::string& str, const std::string_view from, const std::string_view to);

void replaceAll(std::string& str, const std::string_view from, const std::string_view to);

std::vector<std::string> preprocess(const std::string_view in);

