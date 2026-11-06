#include "StringUtils.h"

#include <ctype.h>

#include <algorithm>

void StringUtils::splitString(std::string_view str,
                              char sep,
                              std::vector<std::string_view>& res) {
    size_t pos = 0;

    while (pos < str.size()) {
        const size_t delimiterPos = str.find(sep, pos);

        if (delimiterPos == std::string_view::npos) {
            res.push_back(str.substr(pos));
            pos = str.size();
        } else {
            res.push_back(str.substr(pos, delimiterPos-pos));
            pos = delimiterPos+1;
        }
    }
}

bool StringUtils::equalsIgnoringCase(std::string_view lhs, std::string_view rhs) {
    const auto sameLetter = [](char lhsCharacter, char rhsCharacter) {
        return tolower(static_cast<unsigned char>(lhsCharacter)) == tolower(static_cast<unsigned char>(rhsCharacter));
    };

    return std::ranges::equal(lhs, rhs, sameLetter);
}
