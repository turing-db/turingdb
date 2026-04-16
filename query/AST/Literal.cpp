#include "Literal.h"

#include "CypherAST.h"

using namespace db;

namespace {

bool isHexDigit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
}

uint32_t hexToCodepoint(std::string_view hex) {
    uint32_t cp = 0;
    for (const char c : hex) {
        cp <<= 4;
        if (c >= '0' && c <= '9') cp |= (c - '0');
        else if (c >= 'a' && c <= 'f') cp |= (c - 'a' + 10);
        else cp |= (c - 'A' + 10);
    }
    return cp;
}

void encodeUTF8(uint32_t cp, std::string& out) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<char>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<char>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<char>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0x10FFFF) {
        out.push_back(static_cast<char>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (cp & 0x3F)));
    }
}

bool hasHexDigitsAt(std::string_view raw, size_t pos, size_t count) {
    if (pos + count > raw.size()) {
        return false;
    }
    for (size_t j = 0; j < count; j++) {
        if (!isHexDigit(raw[pos + j])) {
            return false;
        }
    }
    return true;
}

bool hasSurrogatePairAt(std::string_view raw, size_t pos) {
    const bool enoughRoom = pos + 5 < raw.size();
    if (!enoughRoom) {
        return false;
    }
    const bool hasEscapePrefix = raw[pos] == '\\' && raw[pos + 1] == 'u';
    return hasEscapePrefix && hasHexDigitsAt(raw, pos + 2, 4);
}

void unescapeString(std::string_view raw, std::string& out) {
    out.clear();
    out.reserve(raw.size());

    for (size_t i = 0; i < raw.size(); i++) {
        if (raw[i] == '\\' && i + 1 < raw.size()) {
            const char next = raw[i + 1];
            switch (next) {
                case '\\': out.push_back('\\'); i++; break;
                case '\'': out.push_back('\''); i++; break;
                case '"':  out.push_back('"');  i++; break;
                case 't':  out.push_back('\t'); i++; break;
                case 'n':  out.push_back('\n'); i++; break;
                case 'r':  out.push_back('\r'); i++; break;
                case 'b':  out.push_back('\b'); i++; break;
                case 'f':  out.push_back('\f'); i++; break;
                case 'u': {
                    const bool validEscape = hasHexDigitsAt(raw, i + 2, 4);
                    if (!validEscape) {
                        out.push_back(next);
                        i++;
                        break;
                    }

                    const uint32_t high = hexToCodepoint(raw.substr(i + 2, 4));
                    const bool isHighSurrogate = high >= 0xD800 && high <= 0xDBFF;
                    const bool hasLowSurrogate = isHighSurrogate && hasSurrogatePairAt(raw, i + 6);
                    const uint32_t low = hasLowSurrogate ? hexToCodepoint(raw.substr(i + 8, 4)) : 0;
                    const bool validLow = hasLowSurrogate && low >= 0xDC00 && low <= 0xDFFF;

                    const uint32_t cp = validLow ? 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00) : high;
                    i += validLow ? 11 : 5;

                    encodeUTF8(cp, out);
                    break;
                }
                default: out.push_back(next); i++; break;
            }
        } else {
            out.push_back(raw[i]);
        }
    }
}

} // anonymous namespace

NullLiteral* NullLiteral::create(CypherAST* ast) {
    NullLiteral* literal = new NullLiteral();
    ast->addLiteral(literal);
    return literal;
}

BoolLiteral* BoolLiteral::create(CypherAST* ast, bool value) {
    BoolLiteral* literal = new BoolLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

IntegerLiteral* IntegerLiteral::create(CypherAST* ast, int64_t value) {
    IntegerLiteral* literal = new IntegerLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

DoubleLiteral* DoubleLiteral::create(CypherAST* ast, double value) {
    DoubleLiteral* literal = new DoubleLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

StringLiteral::StringLiteral()
{
}

StringLiteral::~StringLiteral() {
}

StringLiteral* StringLiteral::create(CypherAST* ast, std::string_view value) {
    StringLiteral* literal = new StringLiteral();
    unescapeString(value, literal->_value);
    ast->addLiteral(literal);
    return literal;
}

CharLiteral* CharLiteral::create(CypherAST* ast, char value) {
    CharLiteral* literal = new CharLiteral(value);
    ast->addLiteral(literal);
    return literal;
}

MapLiteral::MapLiteral()
{
}

MapLiteral::~MapLiteral() {
}

MapLiteral* MapLiteral::create(CypherAST* ast) {
    MapLiteral* literal = new MapLiteral();
    ast->addLiteral(literal);
    return literal;
}

void MapLiteral::set(Symbol* key, Expr* value) {
    _map[key] = value;
}

EmbeddingLiteral::EmbeddingLiteral(std::vector<float>&& data)
    : _data(std::move(data))
{
}

EmbeddingLiteral::~EmbeddingLiteral() {
}

EmbeddingLiteral* EmbeddingLiteral::create(CypherAST* ast, std::vector<float>&& data) {
    EmbeddingLiteral* literal = new EmbeddingLiteral(std::move(data));
    ast->addLiteral(literal);
    return literal;
}

WildcardLiteral::WildcardLiteral()
{
}

WildcardLiteral::~WildcardLiteral() {
}

WildcardLiteral* WildcardLiteral::create(CypherAST* ast) {
    WildcardLiteral* literal = new WildcardLiteral();
    ast->addLiteral(literal);
    return literal;
}
