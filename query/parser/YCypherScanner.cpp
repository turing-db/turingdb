#include "YCypherScanner.h"

#include <algorithm>

#include "ParserException.h"
#include "CypherError.h"

using namespace db;

void YCypherScanner::advanceLocation(SourceLocation& loc, std::string_view token) {
    _offset = _nextOffset;
    _nextOffset += token.size();
    loc.step();
    loc._endOffset = _nextOffset;

    const size_t lastLineBreak = token.rfind('\n');
    if (lastLineBreak == std::string_view::npos) {
        loc.columns(token.size());
        return;
    }

    loc.lines(std::ranges::count(token, '\n'));
    loc.columns(token.size() - lastLineBreak - 1);
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

void YCypherScanner::holdSyntaxError(const SourceLocation& loc, const std::string& msg) {
    _heldErrorLocation = loc;
    _heldErrorMessage = msg;
}

void YCypherScanner::throwHeldSyntaxError() {
    syntaxError(_heldErrorLocation, _heldErrorMessage);
}

void YCypherScanner::throwUnclosedComment(const SourceLocation& comment) {
    static constexpr uint32_t OPENING_LENGTH = 2;

    SourceLocation opening = comment;
    opening._endLine = opening._beginLine;
    opening._endColumn = opening._beginColumn + OPENING_LENGTH;
    opening._endOffset = opening._beginOffset + OPENING_LENGTH;

    syntaxError(opening, "A comment that opens with /* must close with */");
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

