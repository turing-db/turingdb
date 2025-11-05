#include "StringUtils.h"

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
