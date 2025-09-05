#pragma once

#include "SourceLocation.h"

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "GeneratedCypherParser.h"

namespace db::v2 {

#undef YY_DECL

#define YY_DECL \
    db::v2::YCypherParser::token_type YCypherScanner::lex(db::v2::YCypherParser::semantic_type* yylval, SourceLocation* yylloc)

class YCypherScanner : public yyFlexLexer {
public:
    virtual YCypherParser::token_type lex(YCypherParser::semantic_type* yylval, SourceLocation* yylloc);

    void setQuery(std::string_view query) { _query = query; }

    void allowNotImplemented(bool allowNotImplemented) {
        _allowNotImplemented = allowNotImplemented;
    }

    void advanceLocation(SourceLocation& loc, uint64_t yyleng) {
        _offset = _nextOffset;
        _nextOffset += yyleng;
        loc.step();
        loc.columns(yyleng);
    }

    void locationNewLine(SourceLocation& loc) {
        loc.lines(1);
    }

    [[noreturn]] void syntaxError(const SourceLocation& loc,
                                  const std::string& msg);

    void notImplemented(const SourceLocation& loc,
                        std::string_view rawMsg);

private:
    size_t _nextOffset = 0;
    size_t _offset = 0;
    std::string_view _query;
    bool _allowNotImplemented = true;

    std::string_view getStringView(size_t offset, size_t length) const {
        return _query.substr(offset, length);
    }
};

}
