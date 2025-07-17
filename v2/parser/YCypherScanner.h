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

    void setThrowNotImplemented(bool throwNotImplemented) {
        _throwNotImplemented = throwNotImplemented;
    }

    void advanceLocation(uint64_t yyleng) {
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
    location _location;
    const std::string* _query = nullptr;
    bool _throwNotImplemented = true;
};

}
