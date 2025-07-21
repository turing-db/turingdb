#include "YCypherScanner.h"

#include "ParserException.h"

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

void db::YCypherScanner::generateError(const std::string& msg, std::string& errorOutput, bool printErrorBars) {
    errorOutput.clear();

    if (_query.empty()) {
        errorOutput += "Empty query\n";
        return;
    }


    std::string_view q = _query;
    const auto& loc = _location;

    const size_t firstLine = loc.begin.line;
    const size_t lastLine = loc.end.line;

    if (firstLine != lastLine) {
        // Multi-line error
        size_t lineCount = lastLine - firstLine + 1;
        ignoreLines(q, firstLine - 1);

        for (size_t i = 0; i < lineCount; i++) {
            std::string_view line = nextLine(q);
            const size_t lineNo = firstLine + i;
            std::string prefix = fmt::format("  {:>4} | ", lineNo);

            if (i == 1) {
                const std::string blank(prefix.size() + loc.begin.column - 1, ' ');
                errorOutput += fmt::format("{}⌄ \n", blank, firstLine, line);
            }

            errorOutput += fmt::format("{}{}\n", prefix, line);
        }

        errorOutput += "\n\t" + msg + "\n";
        return;
    }

    const size_t errLineNo = firstLine;

    if (errLineNo != 1) {
        ignoreLines(q, errLineNo - 2);
    }

    std::string_view errLine = nextLine(q);

    if (errLineNo > 1) {
        errorOutput += fmt::format("{:<4} | {}\n", errLineNo - 1, errLine);
        errLine = nextLine(q);
    }

    const size_t errLen = loc.end.column - loc.begin.column;
    const std::string errorBars(errLen, '^');

    std::string prefixLine = fmt::format("{:<4} | ", errLineNo);
    const size_t blankLen = prefixLine.size() + loc.begin.column - 1;
    const std::string blank(blankLen, ' ');

    errorOutput += fmt::format("{}{}\n", prefixLine, errLine);

    if (printErrorBars) {
        errorOutput += fmt::format("{}{}\n", blank, errorBars);
    } else {
        errorOutput += '\n';
    }

    errorOutput += "\t" + msg + "\n\n";
}

void db::YCypherScanner::syntaxError(const std::string& msg) {
    std::string errorMsg;
    generateError(msg, errorMsg);

    throw ParserException(std::move(errorMsg));
}

void db::YCypherScanner::notImplemented(std::string_view rawMsg) {
    if (_allowNotImplemented) {
        return;
    }

    std::string msg = fmt::format("Feature not implemented: {}", rawMsg);
    std::string errorMsg;
    generateError(msg, errorMsg, false);

    throw ParserException(std::move(errorMsg));
}
