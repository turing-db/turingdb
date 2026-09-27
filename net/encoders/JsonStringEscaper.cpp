#include "JsonStringEscaper.h"

#include "ControlCharacters.h"

using namespace db;

namespace {

constexpr std::string_view replacementCharacter = "\xEF\xBF\xBD";

bool isPlainAscii(unsigned char byte) {
    return byte >= 0x20 && byte < 0x80 && byte != '"' && byte != '\\';
}

bool isContinuationByte(std::string_view bytes, size_t index, unsigned char min, unsigned char max) {
    if (index >= bytes.size()) {
        return false;
    }

    const unsigned char byte = static_cast<unsigned char>(bytes[index]);
    return byte >= min && byte <= max;
}

// The second-byte ranges are Unicode's Table 3-7: they exclude overlong forms (E0, F0),
// surrogates (ED) and code points above U+10FFFF (F4).
size_t scanUtf8Sequence(std::string_view bytes, bool& wellFormed) {
    const unsigned char lead = static_cast<unsigned char>(bytes.front());

    size_t length = 0;
    unsigned char secondMin = 0x80;
    unsigned char secondMax = 0xBF;

    if (lead >= 0xC2 && lead <= 0xDF) {
        length = 2;
    } else if (lead == 0xE0) {
        length = 3;
        secondMin = 0xA0;
    } else if (lead == 0xED) {
        length = 3;
        secondMax = 0x9F;
    } else if (lead >= 0xE1 && lead <= 0xEF) {
        length = 3;
    } else if (lead == 0xF0) {
        length = 4;
        secondMin = 0x90;
    } else if (lead >= 0xF1 && lead <= 0xF3) {
        length = 4;
    } else if (lead == 0xF4) {
        length = 4;
        secondMax = 0x8F;
    } else {
        wellFormed = false;
        return 1;
    }

    for (size_t index = 1; index < length; index++) {
        const unsigned char min = (index == 1) ? secondMin : 0x80;
        const unsigned char max = (index == 1) ? secondMax : 0xBF;

        if (!isContinuationByte(bytes, index, min, max)) {
            wellFormed = false;
            return index;
        }
    }

    wellFormed = true;
    return length;
}

void appendEscaped(std::string_view source, std::string& escaped) {
    while (!source.empty()) {
        size_t plainLength = 0;
        while (plainLength < source.size() && isPlainAscii(static_cast<unsigned char>(source[plainLength]))) {
            plainLength++;
        }

        escaped.append(source.substr(0, plainLength));
        source.remove_prefix(plainLength);

        if (source.empty()) {
            return;
        }

        const unsigned char byte = static_cast<unsigned char>(source.front());

        if (byte < 0x20) {
            escaped.append(ControlCharactersEscaper::escapedControlCharacter(static_cast<char>(byte)));
            source.remove_prefix(1);
        } else if (byte == '"' || byte == '\\') {
            escaped.push_back('\\');
            escaped.push_back(static_cast<char>(byte));
            source.remove_prefix(1);
        } else {
            bool wellFormed = false;
            const size_t length = scanUtf8Sequence(source, wellFormed);

            if (wellFormed) {
                escaped.append(source.substr(0, length));
            } else {
                escaped.append(replacementCharacter);
            }

            source.remove_prefix(length);
        }
    }
}

}

void JsonStringEscaper::escape(std::string_view source, std::string& escaped) {
    escaped.clear();
    appendEscaped(source, escaped);
}

void JsonStringEscaper::escapeAndSurroundByQuotes(std::string_view source, std::string& escaped) {
    escaped.clear();
    escaped.push_back('"');
    appendEscaped(source, escaped);
    escaped.push_back('"');
}
