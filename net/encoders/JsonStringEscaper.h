#pragma once

#include <string>
#include <string_view>

namespace db {

// Escapes as ControlCharactersEscaper does, and also replaces ill-formed UTF-8 with U+FFFD:
// JSON must be UTF-8, and a response echoes query text that need not be.
class JsonStringEscaper {
public:
    static void escape(std::string_view source, std::string& escaped);
    static void escapeAndSurroundByQuotes(std::string_view source, std::string& escaped);
};

}
