#pragma once

#include <string.h>
#include <string>
#include <array>

class ControlCharactersEscaper {
public:
    static void escapeAndSurroundByQuotes(std::string_view src, std::string& res) {
        res.resize(src.size() + 1);
        res[0] = '"';
        escapeImpl(src, res, 1);
        res.push_back('"');
    }

    static void escape(std::string_view src, std::string& res) {
        res.resize(src.size());
        escapeImpl(src, res);
    }

    static void escapeImpl(std::string_view src, std::string& res, size_t start = 0) {
        size_t i = start;

        for (const unsigned char c : src) {
            if (c >= _controlMap.size()) {
                if (c == '"') {
                    // Escape double quotes
                    res.resize(res.size() + 2);
                    res[i++] = '\\';
                    res[i++] = '"';

                } else if (c == '\\') {
                    // Escape backslashes
                    res.resize(res.size() + 2);
                    res[i++] = '\\';
                    res[i++] = '\\';

                } else {
                    // Normal character
                    res[i++] = static_cast<char>(c);
                }
            } else {
                // Retrieve the escape sequence for the control character
                // and append it to the result string
                const std::string_view esc = _controlMap[c];

                res.resize(res.size() + esc.size());
                memcpy(res.data() + i, esc.data(), esc.size());
                i += esc.size();
            }
        }

        res.resize(i);
    }

    static constexpr std::string_view escapedControlCharacter(char c) {
        return _controlMap[static_cast<unsigned char>(c)];
    }

private:
    static constexpr std::array<std::string_view, 32> _controlMap = {
        "\\u0000",
        "\\u0001",
        "\\u0002",
        "\\u0003",
        "\\u0004",
        "\\u0005",
        "\\u0006",
        "\\u0007",
        "\\b",
        "\\t",
        "\\n",
        "\\u000B",
        "\\f",
        "\\r",
        "\\u000E",
        "\\u000F",
        "\\u0010",
        "\\u0011",
        "\\u0012",
        "\\u0013",
        "\\u0014",
        "\\u0015",
        "\\u0016",
        "\\u0017",
        "\\u0018",
        "\\u0019",
        "\\u001A",
        "\\u001B",
        "\\u001C",
        "\\u001D",
        "\\u001E",
        "\\u001F",
    };
};
