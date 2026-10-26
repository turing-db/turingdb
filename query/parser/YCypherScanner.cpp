#include "YCypherScanner.h"

#include <algorithm>
#include <string_view>

#include "ParserException.h"
#include "CypherError.h"

using namespace db;

namespace {

size_t skipBlanks(std::string_view query, size_t offset) {
    while (offset < query.size()) {
        const char character = query[offset];
        const bool isSpace = character == ' ' || character == '\t' || character == '\r' || character == '\n';
        const bool opensAComment = query.substr(offset, 2) == "//";

        if (isSpace) {
            offset++;
        } else if (opensAComment) {
            offset = std::min(query.find('\n', offset), query.size());
        } else {
            return offset;
        }
    }

    return query.size();
}

size_t skipQuoted(std::string_view query, size_t offset) {
    const char quote = query[offset];
    const bool escapes = quote != '`';

    offset++;
    while (offset < query.size() && query[offset] != quote) {
        if (escapes && query[offset] == '\\') {
            offset++;
        }

        offset++;
    }

    return offset + 1;
}

size_t findClosingBracket(std::string_view query, size_t bracketOffset) {
    size_t depth = 0;
    size_t offset = bracketOffset;

    while (offset < query.size()) {
        const char character = query[offset];
        const bool opensAQuote = character == '\'' || character == '"' || character == '`';
        const bool opensAComment = query.substr(offset, 2) == "//";

        if (opensAQuote) {
            offset = skipQuoted(query, offset);
        } else if (opensAComment) {
            offset = skipBlanks(query, offset);
        } else if (character == '[') {
            depth++;
            offset++;
        } else if (character == ']' && depth == 1) {
            return offset;
        } else if (character == ']') {
            depth--;
            offset++;
        } else {
            offset++;
        }
    }

    return std::string_view::npos;
}

}

void YCypherScanner::syntaxError(const SourceLocation& loc,
                                 const std::string& msg) {
    std::string errorMsg;

    CypherError err(_query);
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

void YCypherScanner::notImplemented(const SourceLocation& loc,
                                    std::string_view rawMsg) {
    std::string msg = fmt::format("Not implemented: {}", rawMsg);
    std::string errorMsg;

    CypherError err(_query);
    err.setTitle("Cypher parser");
    err.setErrorMsg(msg);
    err.setLocation(loc);
    err.generate(errorMsg);

    throw ParserException(std::move(errorMsg));
}

void YCypherScanner::retractToFirstCharacter(SourceLocation& loc) {
    const size_t handedBack = static_cast<size_t>(yyleng) - 1;

    _nextOffset -= handedBack;
    loc._endColumn -= static_cast<uint32_t>(handedBack);
}

bool YCypherScanner::opensAnEdgeDetail(size_t bracketOffset) const {
    const size_t closingOffset = findClosingBracket(_query, bracketOffset);
    if (closingOffset == std::string_view::npos) {
        return false;
    }

    const size_t tailOffset = skipBlanks(_query, closingOffset + 1);

    return _query.substr(tailOffset, 1) == "-";
}

bool YCypherScanner::continuesAnEdge(size_t offset) const {
    constexpr std::string_view EDGE_CONTINUATIONS = "(>{*+";

    const size_t nextOffset = skipBlanks(_query, offset);

    return nextOffset < _query.size() && EDGE_CONTINUATIONS.contains(_query[nextOffset]);
}
