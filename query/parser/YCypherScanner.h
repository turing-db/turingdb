#pragma once

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "GeneratedCypherParser.h"
#include "SourceLocation.h"

namespace db {

#undef YY_DECL

#define YY_DECL \
    db::YCypherParser::token_type YCypherScanner::lex(db::YCypherParser::semantic_type* yylval, SourceLocation* yylloc)

class YCypherScanner : public yyFlexLexer {
public:
    virtual YCypherParser::token_type lex(YCypherParser::semantic_type* yylval, SourceLocation* yylloc);

    void setQuery(std::string_view query) {
        _query = query;
        _nextOffset = 0;
        _offset = 0;
        _readPos = 0;
    }

    // A token can hold line breaks: a comment, a string, or the gap between the parts of an arrow
    void advanceLocation(SourceLocation& loc, std::string_view token);

    [[noreturn]] void syntaxError(const SourceLocation& loc, const std::string& msg);

    // The parser reports an error before it tries to recover from it, so the report is held
    // until the recovery fails: an error rule that recovers throws a message of its own
    void holdSyntaxError(const SourceLocation& loc, const std::string& msg);
    [[noreturn]] void throwHeldSyntaxError();

    void notImplemented(const SourceLocation& loc, std::string_view rawMsg);

    // Points at the /* of a comment that runs to the end of the query
    [[noreturn]] void throwUnclosedComment(const SourceLocation& comment);

protected:
    /// Allows the scanner to batch copy query string bytes into the @ref _query buffer
    int LexerInput(char* buf, int max_size) override {
        const size_t remaining = _query.size() - _readPos;
        const size_t toRead = std::min<size_t>(max_size, remaining);
        if (toRead == 0) {
            return 0;
        }
        std::memcpy(buf, _query.data() + _readPos, toRead);
        _readPos += toRead;
        return static_cast<int>(toRead);
    }

private:
    size_t _nextOffset {0};
    size_t _offset {0};
    /// Character position in @ref _query which has been consumed so far (inclusive)
    size_t _readPos {0};
    std::string_view _query;
    SourceLocation _heldErrorLocation;
    std::string _heldErrorMessage;

    std::string_view getStringView(size_t offset, size_t length) const {
        return _query.substr(offset, length);
    }
};

}
