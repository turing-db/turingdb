#include "YCypherScanner.h"

#include <sstream>

#include "BioAssert.h"
#include "ParserException.h"

void db::YCypherScanner::generateError(const std::string& msg, std::string& errorOutput, bool printErrorBars) {
    bioassert(_query != nullptr);
    errorOutput.clear();

    const auto& loc = _location;

    const size_t firstLine = loc.begin.line;
    const size_t lastLine = loc.end.line;

    if (firstLine != lastLine) {
        // Multi-line error
        std::istringstream ss(*_query, std::ios_base::in);
        size_t lineCount = lastLine - firstLine + 1;
        std::string line;

        for (size_t i = 0; i < firstLine; i++) {
            ss.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }

        for (size_t i = 0; i < lineCount; i++) {
            const size_t lineNo = firstLine + i;
            std::string prefix = fmt::format("  {:>4} | ", lineNo);

            if (i == 1) {
                const std::string blank(prefix.size() + loc.begin.column - 1, ' ');
                errorOutput += fmt::format("{}⌄ \n", blank, firstLine, line);
            }

            line.clear();
            std::getline(ss, line);
            errorOutput += fmt::format("{}{}\n", prefix, line);
        }

        errorOutput += "\n\t" + msg + "\n";
        return;
    }

    const size_t errLineNo = firstLine;

    std::istringstream ss(*_query, std::ios_base::in);

    if (errLineNo != 1) {
        for (size_t i = 0; i < errLineNo - 2; i++) {
            ss.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    std::string errLine;
    std::getline(ss, errLine);

    if (errLineNo > 1) {
        errorOutput += fmt::format("{:<4} | {}\n", errLineNo - 1, errLine);
        std::getline(ss, errLine);
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
    if (!_throwNotImplemented) {
        return;
    }

    std::string msg = fmt::format("Feature not implemented: {}", rawMsg);
    std::string errorMsg;
    generateError(msg, errorMsg, false);

    throw ParserException(std::move(errorMsg));
}
