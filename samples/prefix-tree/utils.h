#pragma once

#include <vector>
#include <string>
#include <string_view>

void split(std::vector<std::string>& res, std::string_view str, std::string_view delim);

void preprocess(std::vector<std::string>& res, const std::string_view in);

