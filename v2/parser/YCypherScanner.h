#pragma once

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include "GeneratedCypherParser.h"

namespace db {

#undef YY_DECL

#define YY_DECL \
    db::YCypherParser::token_type YCypherScanner::lex(db::YCypherParser::semantic_type* yylval, location* yylloc)

class YCypherScanner : public yyFlexLexer {
public:
    virtual YCypherParser::token_type lex(YCypherParser::semantic_type* yylval, location* yylloc);

    location getLocation() const { return _location; }

    void setQuery(std::string_view query) { _query = query; }

    void allowNotImplemented(bool allowNotImplemented) {
        _allowNotImplemented = allowNotImplemented;
    }

    void advanceLocation(uint64_t yyleng) {
        _offset = _nextOffset;
        _nextOffset += yyleng;
        _location.step();
        _location.columns(yyleng);
    }

    void locationNewLine() {
        _location.lines(1);
    }

    void generateError(const std::string& msg, std::string& errorOutput, bool printErrorBars = true);
    [[noreturn]] void syntaxError(const std::string& msg);
    void notImplemented(std::string_view rawMsg);

private:
    size_t _nextOffset = 0;
    size_t _offset = 0;
    location _location;
    std::string_view _query;
    bool _allowNotImplemented = true;

    std::string_view getStringView(size_t offset, size_t length) const {
        return _query.substr(offset, length);
    }
};

}
