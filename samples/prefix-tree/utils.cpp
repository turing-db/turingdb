#include "utils.h"

#include <cctype>
#include <iterator>
#include <locale>
#include <algorithm>

void split(std::vector<std::string>& res, std::string_view str, std::string_view delim) {
    res.clear();
    if (str.empty()) return;
    size_t l{0};
    size_t r = str.find(delim);

    while (r != std::string::npos) {
        // Add only if the string is non-empty (removes repeated delims)
        if (l != r) res.push_back(std::string(str.substr(l, r - l)));
        l = r + 1;
        r = str.find(delim, l);
    }

    res.push_back(std::string(str.substr(l, std::min(r, str.size()) - l + 1)));
}

inline std::string alphaNumericise(const std::string_view in) {
    std::string out{};

    auto ppxChar = [](char c) {
        if (!std::isalnum(c)) return ' ';
        else if (std::isalpha(c)) return std::tolower(c, std::locale());
        else return c;
    };

    std::transform(std::begin(in), std::end(in), std::back_inserter(out), ppxChar);

    return out;
}


/* PREPROCESSING STEPS
 - replace all non-alphanumeric characters with a whitespace
 - all alphabetical characters are converted to lower case
 - split all strings on whitespace into tokens, which are then returned as a vector
*/
void preprocess(std::vector<std::string>& res, const std::string_view in) {
    const std::string cleaned = alphaNumericise(in);

    split(res, cleaned, " ");
}
