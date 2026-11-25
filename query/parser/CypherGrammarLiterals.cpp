#include "CypherGrammar.h"

#include "Literal.h"
#include "Parameter.h"
#include "Symbol.h"
#include "expr/All.h"

using namespace db;

Expr* CypherGrammar::parseLiteralExpr() {
    const SourcePosition start = currentPosition();
    Literal* literal = parseLiteral();

    Expr* expr = LiteralExpr::create(_ast, literal);
    locate(expr, start);

    return expr;
}

Expr* CypherGrammar::parseParameterExpr() {
    const SourcePosition start = currentPosition();
    Parameter* parameter = parseParameter();

    Expr* expr = ParameterExpr::create(_ast, parameter);
    locate(expr, start);

    return expr;
}

Expr* CypherGrammar::parseLiteralOrParameterExpr(CypherTokenKind literal) {
    if (check(CypherTokenKind::DOLLAR)) {
        return parseParameterExpr();
    } else if (!check(literal)) {
        throwUnexpectedToken();
    }

    return parseLiteralExpr();
}

Literal* CypherGrammar::parseLiteral() {
    if (check(CypherTokenKind::TRUE) || check(CypherTokenKind::FALSE)) {
        return parseBoolLiteral();
    } else if (check(CypherTokenKind::DIGIT) || check(CypherTokenKind::DOUBLE)) {
        return parseNumberLiteral();
    } else if (accept(CypherTokenKind::NULL_)) {
        return NullLiteral::create(_ast);
    } else if (check(CypherTokenKind::STRING_LITERAL)) {
        return parseStringLiteral();
    } else if (check(CypherTokenKind::OPAREN)) {
        return parseEmbeddingLiteral();
    }

    return parseMapLiteral();
}

BoolLiteral* CypherGrammar::parseBoolLiteral() {
    if (accept(CypherTokenKind::TRUE)) {
        return BoolLiteral::create(_ast, true);
    }

    expect(CypherTokenKind::FALSE);

    return BoolLiteral::create(_ast, false);
}

Literal* CypherGrammar::parseNumberLiteral() {
    if (accept(CypherTokenKind::DIGIT)) {
        return IntegerLiteral::create(_ast, _previous._integer);
    }

    expect(CypherTokenKind::DOUBLE);

    return DoubleLiteral::create(_ast, _previous._double);
}

StringLiteral* CypherGrammar::parseStringLiteral() {
    expect(CypherTokenKind::STRING_LITERAL);

    return StringLiteral::create(_ast, _previous._text);
}

MapLiteral* CypherGrammar::parseMapLiteral() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OBRACE);

    if (accept(CypherTokenKind::CBRACE)) {
        MapLiteral* map = MapLiteral::create(_ast);
        locate(map, start);

        return map;
    }

    MapLiteral* map = parseMapPairChain();
    expect(CypherTokenKind::CBRACE);

    return map;
}

MapLiteral* CypherGrammar::parseMapPairChain() {
    const SourcePosition start = currentPosition();

    const std::pair<Symbol*, Expr*> first = parseMapPair();

    MapLiteral* map = MapLiteral::create(_ast);
    map->addEntry(first.first, first.second);
    locate(map, start);

    while (accept(CypherTokenKind::COMMA)) {
        const std::pair<Symbol*, Expr*> pair = parseMapPair();
        map->addEntry(pair.first, pair.second);
    }

    return map;
}

std::pair<Symbol*, Expr*> CypherGrammar::parseMapPair() {
    Symbol* key = parseName();
    expect(CypherTokenKind::COLON);
    Expr* value = parseExpr(ExprFollower::NONE);

    return std::make_pair(key, value);
}

EmbeddingLiteral* CypherGrammar::parseEmbeddingLiteral() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OPAREN);

    EmbeddingLiteral* embedding = parseEmbeddingLiteralItems();
    expect(CypherTokenKind::CPAREN);

    locate(embedding, start);

    return embedding;
}

EmbeddingLiteral* CypherGrammar::parseEmbeddingLiteralItems() {
    const SourcePosition start = currentPosition();

    const double first = parseEmbeddingLiteralItem();
    expect(CypherTokenKind::COMMA);
    const double second = parseEmbeddingLiteralItem();

    EmbeddingLiteral* embedding = EmbeddingLiteral::create(_ast);
    embedding->addItem(first);
    embedding->addItem(second);
    locate(embedding, start);

    while (accept(CypherTokenKind::COMMA)) {
        embedding->addItem(parseEmbeddingLiteralItem());
    }

    return embedding;
}

double CypherGrammar::parseEmbeddingLiteralItem() {
    const bool negative = accept(CypherTokenKind::SUB);

    if (accept(CypherTokenKind::DIGIT)) {
        const int64_t value = _previous._integer;
        return static_cast<double>(negative ? -value : value);
    }

    expect(CypherTokenKind::DOUBLE);
    const double value = _previous._double;

    return negative ? -value : value;
}

Parameter* CypherGrammar::parseParameter() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::DOLLAR);

    Parameter* parameter = nullptr;
    if (accept(CypherTokenKind::DIGIT)) {
        parameter = ParserUtils::createNumberedParameter(_ast, _previous._integer);
    } else {
        Symbol* name = parseName();
        parameter = Parameter::create(_ast, name->getName());
    }

    locate(parameter, start);

    return parameter;
}

Symbol* CypherGrammar::parseName() {
    if (!checkName()) {
        throwUnexpectedToken();
    }

    consume();

    return Symbol::create(_ast, _previous._text);
}

Symbol* CypherGrammar::parseSymbol() {
    if (!checkSymbol()) {
        throwUnexpectedToken();
    }

    consume();

    return Symbol::create(_ast, _previous._text);
}
