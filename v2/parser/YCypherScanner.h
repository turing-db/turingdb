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

    void setQuery(const std::string& query) { _query = &query; }

    void advanceLocation(uint64_t yyleng) {
        _location.step();
        _location.columns(yyleng);
    }

    void locationNewLine() {
        _location.lines(1);
    }

    [[noreturn]] void syntaxError(const std::string& msg);

private:
    location _location;
    const std::string* _query = nullptr;
};

}
