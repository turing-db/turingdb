#include "CypherLexer.h"

#include <stdint.h>
#include <stdlib.h>
#include <algorithm>
#include <array>
#include <charconv>
#include <string>

#include <spdlog/fmt/bundled/format.h>

#include "CypherError.h"
#include "ParserException.h"

using namespace db;

namespace {

struct Keyword {
    std::string_view _spelling;
    CypherTokenKind _kind {CypherTokenKind::ID};
};

constexpr auto KEYWORDS = std::to_array<Keyword>({
    {"ADD", CypherTokenKind::ADD},
    {"ALL", CypherTokenKind::ALL},
    {"ALLREDUCE", CypherTokenKind::ALLREDUCE},
    {"AND", CypherTokenKind::AND},
    {"ANY", CypherTokenKind::ANY},
    {"AS", CypherTokenKind::AS},
    {"ASC", CypherTokenKind::ASC},
    {"ASCENDING", CypherTokenKind::ASCENDING},
    {"AVAILABLE", CypherTokenKind::AVAILABLE},
    {"BY", CypherTokenKind::BY},
    {"CALL", CypherTokenKind::CALL},
    {"CASE", CypherTokenKind::CASE},
    {"CHANGE", CypherTokenKind::CHANGE},
    {"COLLECT", CypherTokenKind::COLLECT},
    {"COMMIT", CypherTokenKind::COMMIT},
    {"CONNECT", CypherTokenKind::CONNECT},
    {"CONSTRAINT", CypherTokenKind::CONSTRAINT},
    {"CONTAINS", CypherTokenKind::CONTAINS},
    {"COSINE", CypherTokenKind::COSINE},
    {"COUNT", CypherTokenKind::COUNT},
    {"CREATE", CypherTokenKind::CREATE},
    {"CSV", CypherTokenKind::CSV},
    {"DATETIMES", CypherTokenKind::DATETIMES},
    {"DELETE", CypherTokenKind::DELETE},
    {"DESC", CypherTokenKind::DESC},
    {"DESCENDING", CypherTokenKind::DESCENDING},
    {"DETACH", CypherTokenKind::DETACH},
    {"DIMENSION", CypherTokenKind::DIMENSION},
    {"DISTINCT", CypherTokenKind::DISTINCT},
    {"DO", CypherTokenKind::DO},
    {"DROP", CypherTokenKind::DROP},
    {"DURATIONS", CypherTokenKind::DURATIONS},
    {"ELSE", CypherTokenKind::ELSE},
    {"EMBEDDING", CypherTokenKind::EMBEDDING},
    {"EMBEDDINGS", CypherTokenKind::EMBEDDINGS},
    {"END", CypherTokenKind::END},
    {"ENDS", CypherTokenKind::ENDS},
    {"ERROR", CypherTokenKind::ERROR_},
    {"EUCLID", CypherTokenKind::EUCLID},
    {"EXISTS", CypherTokenKind::EXISTS},
    {"EXPLAIN", CypherTokenKind::EXPLAIN},
    {"EXTENSIONS", CypherTokenKind::EXTENSIONS},
    {"EXTRACT", CypherTokenKind::EXTRACT},
    {"FAIL", CypherTokenKind::FAIL},
    {"FALSE", CypherTokenKind::FALSE},
    {"FILTER", CypherTokenKind::FILTER},
    {"FLAT", CypherTokenKind::FLAT},
    {"FOR", CypherTokenKind::FOR},
    {"FOREACH", CypherTokenKind::FOREACH},
    {"FROM", CypherTokenKind::FROM},
    {"GML", CypherTokenKind::GML},
    {"GRAPH", CypherTokenKind::GRAPH},
    {"GRAPHS", CypherTokenKind::GRAPHS},
    {"HEADERS", CypherTokenKind::HEADERS},
    {"HNSW", CypherTokenKind::HNSW},
    {"IF", CypherTokenKind::IF},
    {"IN", CypherTokenKind::IN},
    {"INDEX", CypherTokenKind::INDEX},
    {"INDEXES", CypherTokenKind::INDEXES},
    {"INSTALL", CypherTokenKind::INSTALL},
    {"IS", CypherTokenKind::IS},
    {"JSONL", CypherTokenKind::JSONL},
    {"LIMIT", CypherTokenKind::LIMIT},
    {"LIST", CypherTokenKind::LIST},
    {"LOAD", CypherTokenKind::LOAD},
    {"MANDATORY", CypherTokenKind::MANDATORY},
    {"MATCH", CypherTokenKind::MATCH},
    {"MERGE", CypherTokenKind::MERGE},
    {"MERGE_DATAPARTS", CypherTokenKind::MERGE_DATAPARTS},
    {"METRIC", CypherTokenKind::METRIC},
    {"NEW", CypherTokenKind::NEW},
    {"NEXT", CypherTokenKind::NEXT},
    {"NFC", CypherTokenKind::NFC},
    {"NFD", CypherTokenKind::NFD},
    {"NFKC", CypherTokenKind::NFKC},
    {"NFKD", CypherTokenKind::NFKD},
    {"NONE", CypherTokenKind::NONE},
    {"NORMALIZE", CypherTokenKind::NORMALIZE},
    {"NORMALIZED", CypherTokenKind::NORMALIZED},
    {"NOT", CypherTokenKind::NOT},
    {"NULL", CypherTokenKind::NULL_},
    {"OF", CypherTokenKind::OF},
    {"ON", CypherTokenKind::ON},
    {"OPTIONAL", CypherTokenKind::OPTIONAL},
    {"OR", CypherTokenKind::OR},
    {"ORDER", CypherTokenKind::ORDER},
    {"PARQUET", CypherTokenKind::PARQUET},
    {"PROCEDURES", CypherTokenKind::PROCEDURES},
    {"PULL", CypherTokenKind::PULL},
    {"PUSH", CypherTokenKind::PUSH},
    {"REDUCE", CypherTokenKind::REDUCE},
    {"REMOVE", CypherTokenKind::REMOVE},
    {"REQUIRE", CypherTokenKind::REQUIRE},
    {"RETURN", CypherTokenKind::RETURN},
    {"S3", CypherTokenKind::S3},
    {"SCALAR", CypherTokenKind::SCALAR},
    {"SEARCH", CypherTokenKind::SEARCH},
    {"SET", CypherTokenKind::SET},
    {"SHORTESTPATH", CypherTokenKind::SHORTESTPATH},
    {"SHOW", CypherTokenKind::SHOW},
    {"SINGLE", CypherTokenKind::SINGLE},
    {"SKIP", CypherTokenKind::SKIP},
    {"STARTS", CypherTokenKind::STARTS},
    {"SUBMIT", CypherTokenKind::SUBMIT},
    {"THEN", CypherTokenKind::THEN},
    {"TRUE", CypherTokenKind::TRUE},
    {"TYPE", CypherTokenKind::TYPE},
    {"UNION", CypherTokenKind::UNION},
    {"UNIQUE", CypherTokenKind::UNIQUE},
    {"UNWIND", CypherTokenKind::UNWIND},
    {"VECTOR", CypherTokenKind::VECTOR},
    {"WHEN", CypherTokenKind::WHEN},
    {"WHERE", CypherTokenKind::WHERE},
    {"WITH", CypherTokenKind::WITH},
    {"XOR", CypherTokenKind::XOR},
    {"YIELD", CypherTokenKind::YIELD},
});

constexpr size_t MAX_KEYWORD_LENGTH = 15;

constexpr size_t KEYWORD_TABLE_BITS = 11;

constexpr uint8_t EMPTY_SLOT = UINT8_MAX;

constexpr uint32_t FNV_OFFSET_BASIS = 2166136261u;

constexpr uint32_t FNV_PRIME = 16777619u;

static_assert(KEYWORDS.size() + 2 == static_cast<size_t>(CypherTokenKind::ID)
                                         - static_cast<size_t>(CypherTokenKind::PROCEDURES) + 1);

static_assert(KEYWORDS.size() < EMPTY_SLOT);

struct KeywordTable {
    uint32_t _seed {0};
    std::array<uint8_t, size_t {1} << KEYWORD_TABLE_BITS> _slots {};
};

// Clearing bit 0x20 maps a-z onto A-Z. It also changes digits and '_', but never onto a
// letter, so two words fold to the same bytes only if they are equal ignoring case
constexpr uint8_t foldCase(char character) {
    return static_cast<uint8_t>(character & 0xDF);
}

constexpr size_t findSlot(std::string_view word, uint32_t seed) {
    uint32_t hash = FNV_OFFSET_BASIS ^ seed;
    for (const char character : word) {
        hash ^= foldCase(character);
        hash *= FNV_PRIME;
    }

    return hash >> (32 - KEYWORD_TABLE_BITS);
}

constexpr KeywordTable buildKeywordTable() {
    for (uint32_t seed = 0;; seed++) {
        KeywordTable table {._seed = seed};
        table._slots.fill(EMPTY_SLOT);

        bool collides = false;
        for (size_t index = 0; index < KEYWORDS.size() && !collides; index++) {
            uint8_t& slot = table._slots[findSlot(KEYWORDS[index]._spelling, seed)];
            collides = slot != EMPTY_SLOT;
            slot = static_cast<uint8_t>(index);
        }

        if (!collides) {
            return table;
        }
    }
}

constexpr KeywordTable KEYWORD_TABLE = buildKeywordTable();

bool isBreak(char character) {
    return character == ' ' || character == '\t' || character == '\r' || character == '\n';
}

bool isDigit(char character) {
    return '0' <= character && character <= '9';
}

bool isWordStart(char character) {
    return ('a' <= character && character <= 'z') || ('A' <= character && character <= 'Z') || character == '_';
}

bool isWordCharacter(char character) {
    return isWordStart(character) || isDigit(character);
}

bool isFloatSuffix(char character) {
    return character == 'f' || character == 'F' || character == 'd' || character == 'D';
}

bool isHexDigit(char character) {
    return isDigit(character) || ('a' <= character && character <= 'f') || ('A' <= character && character <= 'F');
}

bool isOctalDigit(char character) {
    return '0' <= character && character <= '7';
}

CypherTokenKind findKeyword(std::string_view word) {
    if (word.size() > MAX_KEYWORD_LENGTH) {
        return CypherTokenKind::ID;
    }

    const uint8_t index = KEYWORD_TABLE._slots[findSlot(word, KEYWORD_TABLE._seed)];
    if (index == EMPTY_SLOT) {
        return CypherTokenKind::ID;
    }

    const Keyword& keyword = KEYWORDS[index];
    const bool matches = std::ranges::equal(word, keyword._spelling, {}, foldCase, foldCase);

    return matches ? keyword._kind : CypherTokenKind::ID;
}

bool opensAValueTest(CypherTokenKind kind) {
    return kind == CypherTokenKind::NOT
           || kind == CypherTokenKind::NULL_
           || kind == CypherTokenKind::NORMALIZED
           || kind == CypherTokenKind::NFC
           || kind == CypherTokenKind::NFD
           || kind == CypherTokenKind::NFKC
           || kind == CypherTokenKind::NFKD;
}

}

CypherLexer::CypherLexer(std::string_view query)
    : _query(query)
{
}

CypherLexer::~CypherLexer() {
}

void CypherLexer::lex(CypherToken& token) {
    if (_hasPending) {
        token = _pending;
        _hasPending = false;
    } else {
        lexToken(token);
    }

    if (token._kind != CypherTokenKind::IS) {
        return;
    }

    lexToken(_pending);
    _hasPending = true;

    if (opensAValueTest(_pending._kind)) {
        token._kind = CypherTokenKind::IS_VALUE_TEST;
    }
}

void CypherLexer::throwSyntaxError(const SourceLocation& location, std::string_view message) const {
    std::string report;

    CypherError error(_query);
    error.setTitle("Cypher parser");
    error.setErrorMsg(message);
    error.setLocation(location);
    error.generate(report);

    throw ParserException(std::move(report));
}

void CypherLexer::throwNotImplemented(const SourceLocation& location, std::string_view what) const {
    throwSyntaxError(location, fmt::format("Not implemented: {}", what));
}

void CypherLexer::lexToken(CypherToken& token) {
    while (_offset < _query.size()) {
        const char character = _query[_offset];
        const char next = at(_offset + 1);

        if (isBreak(character)) {
            size_t end = _offset + 1;
            while (end < _query.size() && isBreak(_query[end])) {
                end++;
            }

            advance(end - _offset);
        } else if (character == '/' && (next == '/' || next == '*')) {
            if (!lexComment(token)) {
                return;
            }
        } else if (isWordStart(character)) {
            lexWord(token);
            return;
        } else if (isDigit(character) || (character == '.' && isDigit(next))) {
            lexNumber(token);
            return;
        } else if (character == '"' || character == '\'') {
            lexString(token);
            return;
        } else if (character == '`') {
            lexEscapedName(token);
            return;
        } else {
            lexOperator(token);
            return;
        }
    }

    token = CypherToken {._kind = CypherTokenKind::PROG_END, ._location = _location};
}

bool CypherLexer::lexComment(CypherToken& token) {
    if (_query[_offset + 1] == '/') {
        const size_t lineBreak = _query.find('\n', _offset + 2);
        const size_t end = lineBreak == std::string_view::npos ? _query.size() : lineBreak;
        advance(end - _offset);

        return true;
    }

    const size_t close = _query.find("*/", _offset + 2);
    if (close != std::string_view::npos) {
        advance(close + 2 - _offset);
        return true;
    }

    static constexpr uint32_t OPENING_LENGTH = 2;

    advance(_query.size() - _offset);

    SourceLocation opening = _location;
    opening._endLine = opening._beginLine;
    opening._endColumn = opening._beginColumn + OPENING_LENGTH;
    opening._endOffset = opening._beginOffset + OPENING_LENGTH;

    emitError(token, opening, "A comment that opens with /* must close with */");

    return false;
}

void CypherLexer::lexOperator(CypherToken& token) {
    const char character = _query[_offset];
    const char next = at(_offset + 1);

    switch (character) {
        case '<':
            if (next == '>') {
                emit(token, CypherTokenKind::NOT_EQUAL, 2);
            } else if (next == '=') {
                emit(token, CypherTokenKind::LE, 2);
            } else {
                emit(token, CypherTokenKind::LT, 1);
            }
        break;
        case '>':
            if (next == '=') {
                emit(token, CypherTokenKind::GE, 2);
            } else {
                emit(token, CypherTokenKind::GT, 1);
            }
        break;
        case '+':
            if (next == '=') {
                emit(token, CypherTokenKind::ADD_ASSIGN, 2);
            } else {
                emit(token, CypherTokenKind::PLUS, 1);
            }
        break;
        case '!':
            if (next == '=') {
                emit(token, CypherTokenKind::NOT_EQUAL, 2);
            } else {
                emit(token, CypherTokenKind::EXCLAMATION, 1);
            }
        break;
        case '.':
            if (next == '.') {
                emit(token, CypherTokenKind::RANGE, 2);
            } else {
                emit(token, CypherTokenKind::DOT, 1);
            }
        break;
        case '|':
            if (next == '|') {
                emit(token, CypherTokenKind::CONCAT, 2);
            } else {
                emit(token, CypherTokenKind::PIPE, 1);
            }
        break;
        case '-':
            emit(token, CypherTokenKind::SUB, 1);
        break;
        case ';':
            emit(token, CypherTokenKind::SEMI_COLON, 1);
        break;
        case '(':
            emit(token, CypherTokenKind::OPAREN, 1);
        break;
        case ')':
            emit(token, CypherTokenKind::CPAREN, 1);
        break;
        case '{':
            emit(token, CypherTokenKind::OBRACE, 1);
        break;
        case '}':
            emit(token, CypherTokenKind::CBRACE, 1);
        break;
        case '[':
            emit(token, CypherTokenKind::OBRACK, 1);
        break;
        case ']':
            emit(token, CypherTokenKind::CBRACK, 1);
        break;
        case '=':
            emit(token, CypherTokenKind::ASSIGN, 1);
        break;
        case '$':
            emit(token, CypherTokenKind::DOLLAR, 1);
        break;
        case ',':
            emit(token, CypherTokenKind::COMMA, 1);
        break;
        case ':':
            emit(token, CypherTokenKind::COLON, 1);
        break;
        case '^':
            emit(token, CypherTokenKind::CARET, 1);
        break;
        case '&':
            emit(token, CypherTokenKind::AMPERSAND, 1);
        break;
        case '*':
            emit(token, CypherTokenKind::MULT, 1);
        break;
        case '/':
            emit(token, CypherTokenKind::DIV, 1);
        break;
        case '%':
            emit(token, CypherTokenKind::MOD, 1);
        break;
        default:
            emit(token, CypherTokenKind::UNKNOWN, 1);
        break;
    }
}

void CypherLexer::lexWord(CypherToken& token) {
    size_t end = _offset + 1;
    while (end < _query.size() && isWordCharacter(_query[end])) {
        end++;
    }

    const std::string_view word = _query.substr(_offset, end - _offset);

    emit(token, findKeyword(word), word.size());
    token._text = word;
}

void CypherLexer::lexNumber(CypherToken& token) {
    const size_t start = _offset;
    const size_t integerEnd = skipDigits(start);
    const bool hasIntegerPart = integerEnd > start;
    const size_t exponentEnd = skipExponent(integerEnd);
    const char afterInteger = at(integerEnd);

    size_t doubleEnd = start;
    if (afterInteger == '.' && isDigit(at(integerEnd + 1))) {
        doubleEnd = skipFloatSuffix(skipExponent(skipDigits(integerEnd + 1)));
    } else if (hasIntegerPart && exponentEnd > integerEnd) {
        doubleEnd = skipFloatSuffix(exponentEnd);
    } else if (hasIntegerPart && isFloatSuffix(afterInteger)) {
        doubleEnd = integerEnd + 1;
    }

    if (doubleEnd > start) {
        const std::string lexeme(_query.substr(start, doubleEnd - start));

        emit(token, CypherTokenKind::DOUBLE, lexeme.size());
        token._double = strtod(lexeme.c_str(), nullptr);

        return;
    }

    const bool zeroFirst = _query[start] == '0';
    const char radixLetter = at(start + 1);

    if (zeroFirst && (radixLetter == 'x' || radixLetter == 'X') && isHexDigit(at(start + 2))) {
        size_t end = start + 2;
        while (isHexDigit(at(end))) {
            end++;
        }

        lexRadixInteger(token, end, 16, "Hexadecimal literal exceeded int64 range.");
        return;
    } else if (zeroFirst && (radixLetter == 'o' || radixLetter == 'O') && isOctalDigit(at(start + 2))) {
        size_t end = start + 2;
        while (isOctalDigit(at(end))) {
            end++;
        }

        lexRadixInteger(token, end, 8, "Octal literal exceeded int64 range.");
        return;
    }

    static constexpr uint64_t NEGATED_INT64_MIN = uint64_t {1} << 63;

    uint64_t value = 0;
    const char* begin = _query.data() + start;
    const char* end = _query.data() + integerEnd;
    const auto [_, error] = std::from_chars(begin, end, value, 10);

    advance(integerEnd - start);

    if (error == std::errc::result_out_of_range || value > NEGATED_INT64_MIN) {
        emitError(token, _location, "Decimal literal exceeded int64 range.");
    } else if (value == NEGATED_INT64_MIN) {
        token = CypherToken {._kind = CypherTokenKind::INT64_MIN_MAGNITUDE, ._location = _location};
    } else {
        token = CypherToken {._kind = CypherTokenKind::DIGIT,
                             ._integer = static_cast<int64_t>(value),
                             ._location = _location};
    }
}

void CypherLexer::lexRadixInteger(CypherToken& token, size_t end, int radix, std::string_view overflowMessage) {
    int64_t value = 0;
    const char* digits = _query.data() + _offset + 2;
    const auto [_, error] = std::from_chars(digits, _query.data() + end, value, radix);

    advance(end - _offset);

    if (error == std::errc::result_out_of_range) {
        emitError(token, _location, overflowMessage);
        return;
    }

    token = CypherToken {._kind = CypherTokenKind::DIGIT, ._integer = value, ._location = _location};
}

void CypherLexer::lexString(CypherToken& token) {
    const char quote = _query[_offset];

    size_t position = _offset + 1;
    while (position < _query.size()) {
        const char character = _query[position];

        if (character == quote) {
            const std::string_view text = _query.substr(_offset + 1, position - _offset - 1);

            emit(token, CypherTokenKind::STRING_LITERAL, position + 1 - _offset);
            token._text = text;

            return;
        } else if (character != '\\') {
            position++;
        } else if (position + 1 < _query.size() && _query[position + 1] != '\n') {
            position += 2;
        } else {
            break;
        }
    }

    emit(token, CypherTokenKind::UNKNOWN, 1);
}

void CypherLexer::lexEscapedName(CypherToken& token) {
    const size_t close = _query.find('`', _offset + 1);
    if (close == std::string_view::npos) {
        emit(token, CypherTokenKind::ESC, 1);
        return;
    }

    const std::string_view text = _query.substr(_offset + 1, close - _offset - 1);

    emit(token, CypherTokenKind::ESC_LITERAL, close + 1 - _offset);
    token._text = text;
}

void CypherLexer::advance(size_t length) {
    const std::string_view lexeme = _query.substr(_offset, length);
    _offset += length;

    _location.step();
    _location._endOffset = _offset;

    const size_t lastLineBreak = lexeme.rfind('\n');
    if (lastLineBreak == std::string_view::npos) {
        _location.columns(static_cast<uint32_t>(length));
        return;
    }

    _location.lines(static_cast<uint32_t>(std::ranges::count(lexeme, '\n')));
    _location.columns(static_cast<uint32_t>(length - lastLineBreak - 1));
}

void CypherLexer::emit(CypherToken& token, CypherTokenKind kind, size_t length) {
    advance(length);
    token = CypherToken {._kind = kind, ._location = _location};
}

void CypherLexer::emitError(CypherToken& token, const SourceLocation& location, std::string_view message) {
    token = CypherToken {._kind = CypherTokenKind::LEXER_ERROR, ._text = message, ._location = location};
}

size_t CypherLexer::skipDigits(size_t position) const {
    while (isDigit(at(position))) {
        position++;
    }

    return position;
}

size_t CypherLexer::skipExponent(size_t position) const {
    const char letter = at(position);
    if (letter != 'e' && letter != 'E') {
        return position;
    }

    size_t digits = position + 1;
    const char sign = at(digits);
    if (sign == '+' || sign == '-') {
        digits++;
    }

    if (!isDigit(at(digits))) {
        return position;
    }

    return skipDigits(digits);
}

size_t CypherLexer::skipFloatSuffix(size_t position) const {
    return isFloatSuffix(at(position)) ? position + 1 : position;
}

char CypherLexer::at(size_t position) const {
    return position < _query.size() ? _query[position] : '\0';
}
