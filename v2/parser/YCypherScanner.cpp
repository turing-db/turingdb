#include "YCypherScanner.h"

#include <sstream>

#include "BioAssert.h"
#include "ParserException.h"

void db::YCypherScanner::syntaxError(const std::string& msg) {
    std::string errorMsg;

    // Get error line of query
    const uint32_t errLineNo = _location.begin.line;

    bioassert(_query != nullptr);

    std::istringstream ss(*_query, std::ios_base::in);

    for (size_t i = 1; i < errLineNo; i++) { // Location is 1-indexed
        ss.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }

    std::string errLine;
    std::getline(ss, errLine);

    const size_t errLen = _location.end.column - _location.begin.column;
    const std::string errorBars(errLen, '^');

    std::string prefixLine = fmt::format("Line {}: ", errLineNo);
    const size_t blankLen = prefixLine.size() + _location.begin.column - 1;
    const std::string blank(blankLen, ' ');

    errorMsg = fmt::format("{}{}\n", prefixLine, errLine);
    errorMsg += fmt::format("{}{}\n", blank, errorBars);
    errorMsg += "\t" + msg + "\n\n";

    throw ParserException(std::move(errorMsg));
}
