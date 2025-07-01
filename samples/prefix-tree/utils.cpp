#include "utils.h"

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

/* PREPROCESSING STEPS
 - replace all non-alphanumeric characters with a whitespace
 - all alphabetical characters are converted to lower case
 - split all strings on whitespace into tokens, which are then returned as a vector
*/
std::vector<std::string> preprocess(std::string) {
    std::vector<std::string> tokens{};

    return tokens;
}
