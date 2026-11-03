#pragma once

#include <stddef.h>
#include <string_view>

#include "CypherToken.h"
#include "SourceLocation.h"

namespace db {

class CypherLexer {
public:
    explicit CypherLexer(std::string_view query);
    ~CypherLexer();

    CypherLexer(const CypherLexer&) = delete;
    CypherLexer(CypherLexer&&) = delete;
    CypherLexer& operator=(const CypherLexer&) = delete;
    CypherLexer& operator=(CypherLexer&&) = delete;

    void lex(CypherToken& token);

    [[noreturn]] void throwSyntaxError(const SourceLocation& location, std::string_view message) const;
    [[noreturn]] void throwNotImplemented(const SourceLocation& location, std::string_view what) const;

private:
    std::string_view _query;
    size_t _offset {0};
    SourceLocation _location;
    CypherToken _pending;
    bool _hasPending {false};

    void lexToken(CypherToken& token);
    bool lexComment(CypherToken& token);
    void lexOperator(CypherToken& token);
    void lexWord(CypherToken& token);
    void lexNumber(CypherToken& token);
    void lexRadixInteger(CypherToken& token, size_t end, int radix, std::string_view overflowMessage);
    void lexString(CypherToken& token);
    void lexEscapedName(CypherToken& token);

    void advance(size_t length);
    void emit(CypherToken& token, CypherTokenKind kind, size_t length);
    void emitError(CypherToken& token, const SourceLocation& location, std::string_view message);

    size_t matchArrow(CypherTokenKind& kind) const;
    size_t skipGap(size_t position) const;
    size_t skipDigits(size_t position) const;
    size_t skipExponent(size_t position) const;
    size_t skipFloatSuffix(size_t position) const;
    char at(size_t position) const;
};

}
