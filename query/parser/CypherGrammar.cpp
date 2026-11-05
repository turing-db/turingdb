#include "CypherGrammar.h"

#include <spdlog/fmt/bundled/format.h>

#include "CypherAST.h"
#include "CypherLexer.h"
#include "CypherParser.h"
#include "ParserException.h"
#include "SourceManager.h"

#include "BioAssert.h"

using namespace db;

namespace {

constexpr size_t MAX_EXPECTED_ENTRIES = 4;

struct OpenBracket {
    size_t _index {0};
    CypherTokenKind _closer {CypherTokenKind::PROG_END};
};

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
    const size_t position = _consumedCount - _windowStart + offset;

    while (_window.size() <= position) {
        _lexer->lex(_window.emplace_back());
    }

    return _window[position];
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
    if (_holdsError && _speculations.empty()) {
        _lexer->throwSyntaxError(_heldErrorLocation, _heldErrorMessage);
    }

    const CypherToken& token = peek(0);
    const CypherTokenKind kind = token._kind;
    bioassert(kind != CypherTokenKind::LEXER_ERROR, "A token that failed to lex was consumed");

    if (CypherToken::opensBracket(kind)) {
        enterNesting(token._location);
    } else if (CypherToken::closesBracket(kind)) {
        leaveNesting();
    }

    _previous = token;
    _consumedCount++;

    if (_speculations.empty()) {
        _window.pop_front();
        _windowStart++;
    }

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
    if (_consumedCount == 0) {
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

void CypherGrammar::reportSemanticError(const SourceLocation& location, std::string_view message) {
    if (!_examined && _speculations.empty()) {
        _lexer->throwSyntaxError(location, message);
    } else if (!_holdsError) {
        _holdsError = true;
        _heldErrorLocation = location;
        _heldErrorMessage = message;
    }
}

void CypherGrammar::reportNotImplemented(const SourceLocation& location, std::string_view what) {
    if (!_examined && _speculations.empty()) {
        _lexer->throwNotImplemented(location, what);
    }

    reportSemanticError(location, fmt::format("Not implemented: {}", what));
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
    const uintptr_t stackInUse = _stackBase - reinterpret_cast<uintptr_t>(__builtin_frame_address(0));

    if (_depth == CypherParser::MAX_NESTING_DEPTH) {
        _lexer->throwSyntaxError(location, fmt::format("Query nested deeper than {} levels", CypherParser::MAX_NESTING_DEPTH));
    } else if (stackInUse > CypherParser::MAX_STACK_BYTES) {
        _lexer->throwSyntaxError(location, "Query nested too deeply");
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

// Attempts hold their semantic errors, so that syntax alone chooses the reading. The reading
// chosen is parsed again outside the speculation when it raised one, to report it as usual
void CypherGrammar::parseChoosingBar(const std::function<bool()>& parseRest) {
    if (!_searchedConstructs.insert(_consumedCount).second) {
        parseRest();
        return;
    }

    openSpeculation();
    const size_t speculation = _speculations.size() - 1;
    const bool lastAttemptChosen = chooseBar(speculation, parseRest);

    if (lastAttemptChosen && !_holdsError) {
        closeSpeculation();
        return;
    }

    rewind(speculation);
    closeSpeculation();
    parseRest();
}

// Both readings of [x IN l WHERE x:A|B] parse. As in Neo4j, a `|` after a label is a disjunction
// while a later bar can still close the construct: the reading kept is the one whose bar comes
// last, or the one with no bar when no bar can be read
bool CypherGrammar::chooseBar(size_t speculation, const std::function<bool()>& parseRest) {
    const ReadingOutcome disjunctions = attemptReading(speculation, parseRest);
    const std::vector<size_t> pipes = _speculations[speculation]._tentativePipes;

    if (disjunctions == ReadingOutcome::PARSED_WITH_BAR || pipes.empty()) {
        decidePipes(pipes, pipes.size());
        return disjunctions != ReadingOutcome::FAILED;
    }

    for (size_t bar = pipes.size(); bar > 0; bar--) {
        rewind(speculation);
        decidePipes(pipes, bar - 1);

        if (attemptReading(speculation, parseRest) != ReadingOutcome::FAILED) {
            return true;
        }
    }

    decidePipes(pipes, pipes.size());

    return false;
}

CypherGrammar::ReadingOutcome CypherGrammar::attemptReading(size_t speculation, const std::function<bool()>& parseRest) {
    _speculations[speculation]._tentativePipes.clear();

    try {
        return parseRest() ? ReadingOutcome::PARSED_WITH_BAR : ReadingOutcome::PARSED_WITHOUT_BAR;
    } catch (const ParserException&) {
        return ReadingOutcome::FAILED;
    }
}

void CypherGrammar::openSpeculation() {
    Speculation& speculation = _speculations.emplace_back();

    _ast->saveCheckpoint(speculation._checkpoint);
    speculation._previous = _previous;
    speculation._expected = _expected;
    speculation._pending = _pending;
    speculation._heldErrorLocation = _heldErrorLocation;
    speculation._heldErrorMessage = _heldErrorMessage;
    speculation._consumedCount = _consumedCount;
    speculation._depth = _depth;
    speculation._followerDepth = _followerDepth;
    speculation._operatorCount = _operators.size();
    speculation._operandCount = _operands.size();
    speculation._follower = _follower;
    speculation._examined = _examined;
    speculation._holdsError = _holdsError;
}

void CypherGrammar::rewind(size_t speculation) {
    const Speculation& saved = _speculations[speculation];

    _ast->rollback(saved._checkpoint);
    _previous = saved._previous;
    _expected = saved._expected;
    _pending = saved._pending;
    _heldErrorLocation = saved._heldErrorLocation;
    _heldErrorMessage = saved._heldErrorMessage;
    _consumedCount = saved._consumedCount;
    _depth = saved._depth;
    _followerDepth = saved._followerDepth;
    _operators.resize(saved._operatorCount);
    _operands.resize(saved._operandCount);
    _follower = saved._follower;
    _examined = saved._examined;
    _holdsError = saved._holdsError;
}

void CypherGrammar::closeSpeculation() {
    _speculations.pop_back();

    if (_speculations.empty()) {
        _window.erase(_window.begin(), _window.begin() + (_consumedCount - _windowStart));
        _windowStart = _consumedCount;
    }
}

void CypherGrammar::decidePipes(std::span<const size_t> pipes, size_t bar) {
    for (size_t index = 0; index < pipes.size(); index++) {
        _pipeReadings[pipes[index]] = index == bar ? PipeReading::BAR : PipeReading::DISJUNCTION;
    }
}

size_t CypherGrammar::skipBalanced(size_t offset) {
    bioassert(CypherToken::opensBracket(peek(offset)._kind), "A bracket is skipped from its opener");

    const size_t opener = _consumedCount + offset;
    if (!_closerEnds.contains(opener)) {
        matchBrackets(offset);
    }

    const size_t end = _closerEnds.find(opener)->second;

    return end == 0 ? 0 : end - _consumedCount;
}

void CypherGrammar::matchBrackets(size_t offset) {
    std::vector<OpenBracket> openBrackets;
    size_t position = offset;

    do {
        const CypherTokenKind kind = peek(position)._kind;
        const size_t index = _consumedCount + position;

        if (CypherToken::opensBracket(kind)) {
            const auto known = _closerEnds.find(index);

            if (known == _closerEnds.end()) {
                openBrackets.push_back(OpenBracket {._index = index, ._closer = CypherToken::closerOf(kind)});
                position++;
            } else if (known->second != 0) {
                position = known->second - _consumedCount;
            } else {
                break;
            }
        } else if (!openBrackets.empty() && kind == openBrackets.back()._closer) {
            _closerEnds.emplace(openBrackets.back()._index, index + 1);
            openBrackets.pop_back();
            position++;
        } else if (CypherToken::closesBracket(kind) || kind == CypherTokenKind::PROG_END || kind == CypherTokenKind::LEXER_ERROR) {
            break;
        } else {
            position++;
        }
    } while (!openBrackets.empty());

    for (const OpenBracket& bracket : openBrackets) {
        _closerEnds.emplace(bracket._index, 0);
    }
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
