#pragma once

#if !defined(yyFlexLexerOnce)
#include <FlexLexer.h>
#endif

#include <vector>

#include "GeneratedCypherParser.h"
#include "SourceLocation.h"

namespace db {

#undef YY_DECL

#define YY_DECL \
    db::YCypherParser::token_type YCypherScanner::lexRaw(db::YCypherParser::semantic_type* yylval, SourceLocation* yylloc)

class YCypherScanner : public yyFlexLexer {
public:
    // What the generated scanner matches. The parser reads it through lex(), which keeps
    // the token, since a '-' opening a number is part of it only where the token before
    // it ends no operand.
    virtual YCypherParser::token_type lexRaw(YCypherParser::semantic_type* yylval, SourceLocation* yylloc);

    YCypherParser::token_type lex(YCypherParser::semantic_type* yylval, SourceLocation* yylloc) {
        _lastToken = lexRaw(yylval, yylloc);

        return _lastToken;
    }

    // Which bracket a ']' closes. '-[' and '<-[' open an edge detail and '[' opens an index
    // or a list, and the two nest, as in `-[e {name: names[0]}]-(m)`: popping the one a ']'
    // closes is what tells the tail of `-[:T]-(m)` from the minus of `xs[0] - (1 + 2)`.
    enum class BracketKind {
        EdgeDetail,
        Index,
    };

    // Whether the token before this one ends an operand, which makes a '-' written against
    // it the subtraction operator: `4-1` is three tokens where `[1, -2]` holds two.
    bool subtractsFromTheLastToken() const;

    BracketKind closeBracket();

    void openBracket(BracketKind kind) { _openBrackets.push_back(kind); }

    // Give back what a rule matched past its first character, leaving the location where
    // that character ends.
    void retractToFirstCharacter(SourceLocation& loc, uint64_t yyleng) {
        const uint64_t handedBack = yyleng - 1;

        _nextOffset -= handedBack;
        loc._endColumn -= static_cast<uint32_t>(handedBack);
    }

    void setQuery(std::string_view query) {
        _query = query;
        _lastToken = YCypherParser::token::PROG_END;
        _nextOffset = 0;
        _offset = 0;
        _readPos = 0;
        _openBrackets.clear();
    }

    void advanceLocation(SourceLocation& loc, uint64_t yyleng) {
        _offset = _nextOffset;
        _nextOffset += yyleng;
        loc.step();
        loc.columns(yyleng);
    }

    static void locationNewLine(SourceLocation& loc) { loc.lines(1); }

    [[noreturn]] void syntaxError(const SourceLocation& loc, const std::string& msg);

    void notImplemented(const SourceLocation& loc, std::string_view rawMsg);

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
    YCypherParser::token_type _lastToken {YCypherParser::token::PROG_END};

    std::vector<BracketKind> _openBrackets;

    size_t _nextOffset {0};
    size_t _offset {0};
    /// Character position in @ref _query which has been consumed so far (inclusive)
    size_t _readPos {0};
    std::string_view _query;

    std::string_view getStringView(size_t offset, size_t length) const {
        return _query.substr(offset, length);
    }
};

}
