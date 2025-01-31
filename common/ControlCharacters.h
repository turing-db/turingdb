#pragma once

#include <string>

class ControlCharactersEscaper {
public:
    static void escape(const std::string& src, std::string& result) {
        result.clear();
        for (const char c : src) {
            switch (c) {
                case '\a': {
                    result += "\\a";
                    break;
                }
                case '\x08': {
                    result += "\\b";
                    break;
                }
                case '\t': {
                    result += "\\t";
                    break;
                }
                case '\n': {
                    result += "\\n";
                    break;
                }
                case '\v': {
                    result += "\\v";
                    break;
                }
                case '\f': {
                    result += "\\f";
                    break;
                }
                case '\r': {
                    result += "\\r";
                    break;
                }
                case '\"': {
                    result += "\\\"";
                    break;
                }
                case '\\': {
                    result += "\\\\";
                    break;
                }
                default: {
                    result += c;
                    break;
                }
            }
        }
    }
};
