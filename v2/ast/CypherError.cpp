#include "CypherError.h"

using namespace db;

namespace {

constexpr std::string_view emptyPrefix = "       | ";
constexpr std::string_view metaPrefix = "-------* ";
constexpr std::string_view prefixFmt = "  {:>4} | ";

std::string_view nextLine(std::string_view& str) {
    const auto pos = str.find('\n');
    if (pos == std::string_view::npos) {
        auto line = str;
        str = str.substr(str.size(), str.size());

        return line;
    }

    auto line = str.substr(0, pos);
    str = str.substr(pos + 1, str.size());
    return line;
}

void ignoreLines(std::string_view& str, size_t count) {
    for (size_t i = 0; i < count; i++) {
        nextLine(str);
    }
}

void errorBars(std::string& errorOutput,
               size_t blankLen,
               size_t errLen,
               std::string_view character = "^") {

    errorOutput += emptyPrefix;

    for (size_t i = 0; i < blankLen; i++) {
        errorOutput += ' ';
    }

    for (size_t i = 0; i < errLen; i++) {
        errorOutput += character;
    }

    errorOutput += "\n";
}

}

void CypherError::generate(std::string& errorOutput) {
    if (!_title.empty()) {
        errorOutput += metaPrefix;
        errorOutput += _title;
        errorOutput += "\n";
    }

    if (_query.empty()) {
        errorOutput += emptyPrefix;
        errorOutput += "Empty query\n";
        return;
    }

    if (!_loc) {
        errorOutput += emptyPrefix;
        errorOutput += "Unknown location\n";
        errorOutput += emptyPrefix;
        errorOutput += _errorMsg;
        return;
    }

    const size_t firstLineNo = _loc->beginLine;
    const size_t lastLineNo = _loc->endLine;

    if (firstLineNo != lastLineNo) {
        // Multi-line error
        size_t lineCount = lastLineNo - firstLineNo + 1;
        ignoreLines(_query, firstLineNo - 1);

        std::string_view line = nextLine(_query);
        std::string prefix = fmt::format(prefixFmt, firstLineNo);
        errorBars(errorOutput,
                  _loc->beginColumn - 1,
                  line.size() - _loc->beginColumn - 1,
                  "⌄");

        errorOutput += fmt::format("{}{}\n", prefix, line);

        for (size_t i = 1; i < lineCount; i++) {
            line = nextLine(_query);
            const size_t lineNo = firstLineNo + i;
            prefix = fmt::format(prefixFmt, lineNo);
            errorOutput += fmt::format("{}{}\n", prefix, line);
        }

        errorBars(errorOutput, 0, _loc->endColumn - 1, "^");

        errorOutput += metaPrefix;
        errorOutput += _errorMsg;
        errorOutput += "\n";
        return;
    }

    const size_t errLineNo = firstLineNo;

    if (errLineNo != 1) {
        ignoreLines(_query, errLineNo - 2);
    }

    std::string_view errLine = nextLine(_query);

    if (errLineNo > 1) {
        errorOutput += fmt::format(prefixFmt, errLineNo - 1);
        errorOutput += errLine;
        errorOutput += "\n";
        errLine = nextLine(_query);
    }

    const size_t errLen = _loc->endColumn - _loc->beginColumn;

    std::string prefixLine = fmt::format(prefixFmt, errLineNo);
    const size_t blankLen =  _loc->beginColumn - 1;
    const std::string blank(blankLen, ' ');

    errorOutput += fmt::format("{}{}\n", prefixLine, errLine);
    errorBars(errorOutput, _loc->beginColumn - 1, errLen);

    errorOutput += metaPrefix;
    errorOutput += _errorMsg;
    errorOutput += "\n";
}
