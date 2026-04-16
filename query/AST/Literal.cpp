#include "Literal.h"

#include "CypherAST.h"

using namespace db;

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

StringLiteral::StringLiteral(std::string_view raw) {
    _value.reserve(raw.size());

    for (size_t i = 0; i < raw.size(); i++) {
        if (raw[i] == '\\' && i + 1 < raw.size()) {
            char next = raw[i + 1];
            switch (next) {
                case '\\': _value.push_back('\\'); i++; break;
                case '\'': _value.push_back('\''); i++; break;
                case '"':  _value.push_back('"');  i++; break;
                case 't':  _value.push_back('\t'); i++; break;
                case 'n':  _value.push_back('\n'); i++; break;
                case 'r':  _value.push_back('\r'); i++; break;
                case 'b':  _value.push_back('\b'); i++; break;
                case 'f':  _value.push_back('\f'); i++; break;
                default:   _value.push_back(raw[i]); break;
            }
        } else {
            _value.push_back(raw[i]);
        }
    }
}

StringLiteral* StringLiteral::create(CypherAST* ast, std::string_view value) {
    StringLiteral* literal = new StringLiteral(value);
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
