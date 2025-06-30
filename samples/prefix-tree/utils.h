#pragma once


#include <vector>
#include <string>

enum CMD {
    INSERT,
    PRINT,
    REMOVE
};

std::vector<std::string> split(std::string_view str, std::string_view delim) {
    std::vector<std::string> out{};

    size_t l{0};
    size_t r = str.find(delim);

    while (r != std::string::npos) {
        out.push_back(std::string(str.substr(l, r - l)));
        l = r + 1;
        r = str.find(delim, l);
    }

    out.push_back(std::string(str.substr(l, std::min(r, str.size()) - l + 1)));

    return out;
}

