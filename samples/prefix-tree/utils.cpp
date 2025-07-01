#include "utils.h"
#include <cctype>
#include <iostream>
#include <iterator>
#include <locale>

std::vector<std::string> split(std::string_view str, std::string_view delim) {
    std::vector<std::string> out{};

    size_t l{0};
    size_t r = str.find(delim);

    while (r != std::string::npos) {
        // Add only if the string is non-empty (removes repeated delims)
        if (l != r) out.push_back(std::string(str.substr(l, r - l)));
        l = r + 1;
        r = str.find(delim, l);
    }

    out.push_back(std::string(str.substr(l, std::min(r, str.size()) - l + 1)));

    return out;
}

bool replace(std::string& str, const std::string_view from, const std::string_view to) {
    size_t start_pos = str.find(from);

    if (start_pos == std::string::npos) return false;

    str.replace(start_pos, from.length(), to);
    return true;
}

void replaceAll(std::string& str, const std::string_view from, const std::string_view to) {
    if (from.empty()) return;
    size_t start_pos = 0;
    while((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();
    }
}
    

/* PREPROCESSING STEPS
 - replace all non-alphanumeric characters with a whitespace
 - all alphabetical characters are converted to lower case
 - split all strings on whitespace into tokens, which are then returned as a vector
*/
std::vector<std::string> preprocess(const std::string_view in) {
    std::vector<std::string> tokens{};
    std::string out{};

    auto ppxChar = [](char c) {
        if (!std::isalnum(c)) return ' ';
        else if (std::isalpha(c)) return std::tolower(c, std::locale());
        else return c;
    };

    std::transform(std::begin(in), std::end(in), std::back_inserter(out), ppxChar);
    tokens = split(out, " ");

    return tokens;
}
