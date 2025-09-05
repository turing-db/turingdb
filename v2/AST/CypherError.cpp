#include "CypherError.h"

using namespace db::v2;

namespace {

constexpr std::string_view EMPTY_PREFIX = "       | ";
constexpr std::string_view META_PREFIX = "-------* ";
constexpr std::string_view PREFIX_FMT = "  {:>4} | ";
constexpr char16_t DOWN_ERR_CHAR = u'⌄';
constexpr char16_t UP_ERR_CHAR = u'^';

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

struct LinePatternLocation {
    size_t beginLine {};
    size_t count {};
};

template <char16_t Char>
std::string errorBars(const LinePatternLocation& loc) {
    constexpr std::string_view charStr = Char == UP_ERR_CHAR ? "^" : "⌄";

    std::string errBar;

    errBar += EMPTY_PREFIX;

    for (size_t i = 0; i < loc.beginLine; i++) {
        errBar += ' ';
    }

    for (size_t i = 0; i < loc.count; i++) {
        errBar += charStr;
    }

    errBar += "\n";
    return errBar;
}

std::string lineNoPrefix(size_t lineNo) {
    return fmt::format(PREFIX_FMT, lineNo);
}

}

void CypherError::generate(std::string& errorOutput) {
    if (!_title.empty()) {
        errorOutput += META_PREFIX;
        errorOutput += _title;
        errorOutput += "\n";
    }

    if (_query.empty()) {
        errorOutput += EMPTY_PREFIX;
        errorOutput += "Empty query\n";
    }

    if (!_loc) {
        errorOutput += EMPTY_PREFIX;
        errorOutput += _errorMsg;
        return;
    }

    const size_t firstLineNo = _loc->beginLine;
    const size_t lastLineNo = _loc->endLine;

    if (firstLineNo != lastLineNo) {
        return generateMultiLine(errorOutput);
    } else {
        return generateSingleLine(errorOutput);
    }
}

void CypherError::generateSingleLine(std::string& errorOutput) {
    const size_t firstLineNo = _loc->beginLine;
    const size_t errLineNo = firstLineNo;

    if (errLineNo != 1) {
        ignoreLines(_query, errLineNo - 2);
    }

    std::string_view errLine = nextLine(_query);

    if (errLineNo > 1) {
        errorOutput += lineNoPrefix(errLineNo - 1);
        errorOutput += errLine;
        errorOutput += "\n";
        errLine = nextLine(_query);
    }

    errorOutput += lineNoPrefix(firstLineNo);
    errorOutput += errLine;
    errorOutput += "\n";

    const LinePatternLocation errLineLoc = {
        .beginLine = _loc->beginColumn - 1,
        .count = _loc->endColumn - _loc->beginColumn,
    };

    errorOutput += errorBars<UP_ERR_CHAR>(errLineLoc);
    errorOutput += META_PREFIX;
    errorOutput += _errorMsg;
    errorOutput += "\n";
}

void CypherError::generateMultiLine(std::string& errorOutput) {
    // Multi-line error
    const size_t firstLineNo = _loc->beginLine;
    const size_t lastLineNo = _loc->endLine;

    ignoreLines(_query, firstLineNo - 1);

    std::string_view line = nextLine(_query);

    const LinePatternLocation firstLineLoc = {
        .beginLine = _loc->beginColumn - 1,
        .count = line.size() - _loc->beginColumn + 1,
    };

    errorOutput += errorBars<DOWN_ERR_CHAR>(firstLineLoc);
    errorOutput += lineNoPrefix(firstLineNo);
    errorOutput += line;
    errorOutput += "\n";

    for (size_t i = 1; i < lastLineNo - firstLineNo + 1; i++) {
        line = nextLine(_query);
        errorOutput += lineNoPrefix(firstLineNo + i);
        errorOutput += line;
        errorOutput += "\n";
    }

    const LinePatternLocation lastLineLoc = {
        .beginLine = 0,
        .count = _loc->endColumn + 1,
    };

    errorOutput += errorBars<UP_ERR_CHAR>(lastLineLoc);
    errorOutput += META_PREFIX;
    errorOutput += _errorMsg;
    errorOutput += "\n";
}
