#include "CypherGrammar.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "CypherLexer.h"
#include "CypherParser.h"
#include "SourceManager.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t MAX_EXPECTED_ENTRIES = 4;

bool opensBracket(CypherTokenKind kind) {
    return kind == CypherTokenKind::OPAREN
           || kind == CypherTokenKind::OBRACK
           || kind == CypherTokenKind::OBRACE
           || kind == CypherTokenKind::TIP_TAIL_BRACKET
           || kind == CypherTokenKind::TAIL_BRACKET;
}

bool closesBracket(CypherTokenKind kind) {
    return kind == CypherTokenKind::CPAREN
           || kind == CypherTokenKind::CBRACK
           || kind == CypherTokenKind::CBRACE;
}

}

CypherGrammar::CypherGrammar(CypherAST* ast, CypherLexer* lexer)
    : _ast(ast),
    _sourceManager(ast->getSourceManager()),
    _lexer(lexer)
{
}

CypherGrammar::~CypherGrammar() {
}

const CypherToken& CypherGrammar::peek(size_t offset) {
    while (_window.size() <= offset) {
        _lexer->lex(_window.emplace_back());
    }

    return _window[offset];
}

const CypherToken& CypherGrammar::examine() {
    const CypherToken& token = peek(0);
    _examined = true;

    if (token._kind == CypherTokenKind::LEXER_ERROR) {
        _lexer->throwSyntaxError(token._location, token._text);
    }

    return token;
}

CypherTokenKind CypherGrammar::examineKind() {
    return examine()._kind;
}

bool CypherGrammar::check(CypherTokenKind kind) {
    _expected.set(static_cast<size_t>(kind));
    return examineKind() == kind;
}

bool CypherGrammar::accept(CypherTokenKind kind) {
    if (!check(kind)) {
        return false;
    }

    consume();
    return true;
}

void CypherGrammar::expect(CypherTokenKind kind) {
    if (!check(kind)) {
        throwUnexpectedToken();
    }

    consume();
}

bool CypherGrammar::checkName() {
    _expected |= getNameKinds();
    return CypherToken::isName(examineKind());
}

bool CypherGrammar::checkSymbol() {
    _expected |= getSymbolKinds();
    return CypherToken::isSymbol(examineKind());
}

void CypherGrammar::consume() {
    if (_holdsError) {
        _lexer->throwSyntaxError(_heldErrorLocation, _heldErrorMessage);
    }

    const CypherToken& token = peek(0);
    const CypherTokenKind kind = token._kind;
    bioassert(kind != CypherTokenKind::LEXER_ERROR, "A token that failed to lex was consumed");

    if (opensBracket(kind)) {
        enterNesting(token._location);
    } else if (closesBracket(kind)) {
        leaveNesting();
    }

    _previous = token;
    _consumedAny = true;
    _window.pop_front();

    _expected = _pending;
    _pending.reset();
    _examined = false;
}

void CypherGrammar::addPending(CypherTokenKind kind) {
    _pending.set(static_cast<size_t>(kind));
}

SourceLocation CypherGrammar::currentLocation() {
    return peek(0)._location;
}

CypherGrammar::SourcePosition CypherGrammar::currentPosition() {
    const SourceLocation& location = peek(0)._location;

    return SourcePosition {._line = location._beginLine, ._column = location._beginColumn, ._offset = location._beginOffset};
}

SourceLocation CypherGrammar::locationFrom(const SourcePosition& start) const {
    const SourceLocation& end = _previous._location;

    return SourceLocation {
        ._beginLine = start._line,
        ._beginColumn = start._column,
        ._endLine = end._endLine,
        ._endColumn = end._endColumn,
        ._beginOffset = start._offset,
        ._endOffset = end._endOffset,
    };
}

void CypherGrammar::locate(const void* object, const SourcePosition& start) {
    _sourceManager->setLocation(object, locationFrom(start));
}

SourceLocation CypherGrammar::emptyLocation() const {
    if (!_consumedAny) {
        return SourceLocation {};
    }

    SourceLocation location = _previous._location;
    location.step();

    return location;
}

void CypherGrammar::throwUnexpectedToken() {
    throwUnexpected(peek(0), _expected);
}

void CypherGrammar::throwUnexpectedTokenAt(size_t offset, const TokenKinds& kinds) {
    if (offset == 0) {
        throwUnexpected(peek(0), kinds | _expected);
    } else if (_holdsError) {
        _lexer->throwSyntaxError(_heldErrorLocation, _heldErrorMessage);
    }

    const CypherToken& token = peek(offset);
    if (token._kind == CypherTokenKind::LEXER_ERROR) {
        _lexer->throwSyntaxError(token._location, token._text);
    }

    throwUnexpected(token, offset == 1 ? kinds | _pending : kinds);
}

void CypherGrammar::throwSemanticError(const SourceLocation& location, std::string_view message) {
    if (!_examined) {
        _lexer->throwSyntaxError(location, message);
    } else if (!_holdsError) {
        _holdsError = true;
        _heldErrorLocation = location;
        _heldErrorMessage = message;
    }
}

void CypherGrammar::throwNotImplemented(const SourceLocation& location, std::string_view what) {
    if (!_examined) {
        _lexer->throwNotImplemented(location, what);
    }

    throwSemanticError(location, fmt::format("Not implemented: {}", what));
}

void CypherGrammar::throwUnexpected(const CypherToken& token, const TokenKinds& kinds) {
    const bool expectsName = kinds.test(static_cast<size_t>(CypherTokenKind::ID));

    std::vector<std::string_view> entries;
    bool nameListed = false;
    for (size_t index = 0; index < CypherToken::KIND_COUNT; index++) {
        if (!kinds.test(index)) {
            continue;
        }

        const CypherTokenKind kind = static_cast<CypherTokenKind>(index);
        if (!expectsName || !CypherToken::isName(kind)) {
            entries.push_back(CypherToken::toString(kind));
        } else if (!nameListed) {
            entries.push_back("a name");
            nameListed = true;
        }
    }

    std::string message = "syntax error, unexpected ";
    message += CypherToken::toString(token._kind);

    if (!entries.empty() && entries.size() <= MAX_EXPECTED_ENTRIES) {
        message += ", expecting ";
        message += entries.front();

        for (size_t entry = 1; entry < entries.size(); entry++) {
            message += " or ";
            message += entries[entry];
        }
    }

    _lexer->throwSyntaxError(token._location, message);
}

void CypherGrammar::enterNesting(const SourceLocation& location) {
    if (_depth == CypherParser::MAX_NESTING_DEPTH) {
        _lexer->throwSyntaxError(location, fmt::format("Query nested deeper than {} levels", CypherParser::MAX_NESTING_DEPTH));
    }

    _depth++;
}

void CypherGrammar::leaveNesting() {
    bioassert(_depth > 0, "A bracket closed more levels than were opened");
    _depth--;
}

bool CypherGrammar::isFollowedBy(ExprFollower follower) const {
    return _follower == follower && _followerDepth == _depth;
}

const CypherGrammar::TokenKinds& CypherGrammar::getNameKinds() {
    static const TokenKinds names = [] {
        TokenKinds kinds;
        for (size_t index = 0; index < CypherToken::KIND_COUNT; index++) {
            if (CypherToken::isName(static_cast<CypherTokenKind>(index))) {
                kinds.set(index);
            }
        }

        return kinds;
    }();

    return names;
}

const CypherGrammar::TokenKinds& CypherGrammar::getSymbolKinds() {
    static const TokenKinds symbols = [] {
        TokenKinds kinds;
        for (size_t index = 0; index < CypherToken::KIND_COUNT; index++) {
            if (CypherToken::isSymbol(static_cast<CypherTokenKind>(index))) {
                kinds.set(index);
            }
        }

        return kinds;
    }();

    return symbols;
}

const CypherGrammar::TokenKinds& CypherGrammar::getCallOpeningKinds() {
    static const TokenKinds openings = getSymbolKinds() | kindsOf({CypherTokenKind::OPAREN, CypherTokenKind::OBRACE});

    return openings;
}

CypherGrammar::TokenKinds CypherGrammar::kindsOf(std::initializer_list<CypherTokenKind> kinds) {
    TokenKinds set;
    for (const CypherTokenKind kind : kinds) {
        set.set(static_cast<size_t>(kind));
    }

    return set;
}
