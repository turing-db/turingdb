#pragma once

#include "HTTPParsingInfo.h"

namespace net {

class URIParser {
public:
    /*
     * @brief Default implementation
     *
     * - Endpoint index is always set to 0
     * - Each parameter value is stored in order encountered
     * */
    static HTTP::Result<void> parseURI(HTTP::Info& info, std::string_view uri) {
        // Extract the path part of the URI
        // up to the ? character if any
        const char* pathBegin = uri.data();
        const char* pathPtr = pathBegin;
        const char* const uriEnd = pathPtr + uri.size();
        for (; pathPtr < uriEnd; pathPtr++) {
            if (*pathPtr == '?') {
                break;
            }
        }

        info._path = std::string_view(pathBegin, pathPtr - pathBegin);
        info._endpoint = 0;

        // We can stop here if we are already at the end of the URI
        if (pathPtr >= uriEnd) {
            return {};
        }

        // URI variables
        pathPtr++;
        auto& parameters = info._params;
        std::string_view key;
        std::string_view value;

        const char* wordStart = pathPtr;
        size_t i = 0;
        for (; pathPtr < uriEnd; pathPtr++) {
            const char c = *pathPtr;
            if (c == '=') {
                key = std::string_view(wordStart, pathPtr - wordStart);
                value = std::string_view();
                wordStart = pathPtr + 1;
            } else if (c == '&') {
                value = std::string_view(wordStart, pathPtr - wordStart);
                if (!key.empty() && !value.empty()) {
                    if (i > HTTP::MAX_PARAM_COUNT) {
                        return BadResult(HTTP::Error::TOO_MANY_PARAMS);
                    }
                    parameters[i] = value;
                    ++i;
                }

                key = std::string_view();
                value = std::string_view();
                wordStart = pathPtr + 1;
            }
        }

        if (wordStart < uriEnd && !key.empty()) {
            value = std::string_view(wordStart, uriEnd - wordStart);
            if (i > HTTP::MAX_PARAM_COUNT) {
                return BadResult(HTTP::Error::TOO_MANY_PARAMS);
            }
            parameters[i] = value;
            ++i;
        }

        return {};
    };
};

}
