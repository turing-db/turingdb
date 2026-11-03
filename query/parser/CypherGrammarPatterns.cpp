#include "CypherGrammar.h"

#include "CypherAST.h"
#include "EdgePattern.h"
#include "LabelExpr.h"
#include "Literal.h"
#include "NodePattern.h"
#include "Parameter.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QuantifiedPath.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "WhereClause.h"
#include "expr/ExistsExpr.h"
#include "expr/Expr.h"

using namespace db;

namespace {

constexpr std::string_view QUANTIFIER_TWICE_MESSAGE = "A relationship takes one length quantifier: "
                                                      "either [e*1..3] inside the brackets or {1,3} after them";

constexpr std::string_view NESTED_QUANTIFIER_MESSAGE = "A quantified pattern cannot repeat a pattern that is quantified itself";

constexpr std::string_view HOPLESS_QUANTIFIER_MESSAGE = "A quantified pattern repeats at least one relationship";

}

Pattern* CypherGrammar::parsePattern() {
    const SourcePosition start = currentPosition();

    PatternElement* first = parsePatternPart();

    Pattern* pattern = Pattern::create(_ast);
    pattern->addElement(first);
    locate(pattern, start);

    while (accept(CypherTokenKind::COMMA)) {
        pattern->addElement(parsePatternPart());
        locate(pattern, start);
    }

    return pattern;
}

Pattern* CypherGrammar::parsePatternWhere() {
    const SourcePosition start = currentPosition();

    Pattern* pattern = parsePattern();

    if (check(CypherTokenKind::WHERE)) {
        WhereClause* where = parseWhereClause(ExprFollower::NONE);
        pattern->setWhere(where);
    }

    ParserUtils::foldEntityWheres(_ast, pattern);
    locate(pattern, start);

    return pattern;
}

PatternElement* CypherGrammar::parsePatternPart() {
    if (checkName()) {
        return parsePatternAlias();
    }

    return parsePatternElem();
}

PatternElement* CypherGrammar::parsePatternAlias() {
    const SourcePosition start = currentPosition();

    Symbol* name = parseName();
    expect(CypherTokenKind::ASSIGN);
    PatternElement* element = parsePatternElem();

    element->setPathSymbol(name);
    locate(element, start);

    return element;
}

PatternElement* CypherGrammar::parsePatternElem() {
    const SourcePosition start = currentPosition();

    NodePattern* node = parseNodePattern();

    PatternElement* element = PatternElement::create(_ast);
    element->addEntity(node);
    locate(element, start);

    while (startsPatternElemChain()) {
        const std::pair<EdgePattern*, NodePattern*> hop = parsePatternElemChain();
        element->addEntity(hop.first);
        element->addEntity(hop.second);
    }

    return element;
}

std::pair<EdgePattern*, NodePattern*> CypherGrammar::parsePatternElemChain() {
    if (!check(CypherTokenKind::OPAREN)) {
        const SourcePosition edgeStart = currentPosition();
        EdgePattern* edge = parseEdgePattern();
        const SourceLocation edgeLocation = locationFrom(edgeStart);

        const SourcePosition quantifierStart = currentPosition();
        QuantifiedPath* quantifier = parseOptionalQuantifiedPath();
        const SourceLocation quantifierLocation = quantifier ? locationFrom(quantifierStart) : SourceLocation {};

        NodePattern* node = parseNodePattern();

        if (quantifier) {
            if (edge->getQuantifiedPath()) {
                throwSemanticError(quantifierLocation, QUANTIFIER_TWICE_MESSAGE);
            }

            edge->setQuantifiedPath(quantifier);
        }

        return std::make_pair(ParserUtils::createQuantifiedWalk(_ast, edge, edgeLocation), node);
    }

    consume();

    const SourcePosition bodyStart = currentPosition();
    PatternElement* body = parsePatternElem();
    const SourceLocation bodyLocation = locationFrom(bodyStart);

    WhereClause* where = parseOptionalWhereClause();
    expect(CypherTokenKind::CPAREN);

    QuantifiedPath* quantifier = parseQuantifiedPath();
    NodePattern* node = parseNodePattern();

    const PatternElement::EntityPatterns& entities = body->getEntities();
    const size_t hopCount = entities.size() / 2;

    if (hopCount == 0) {
        throwSemanticError(bodyLocation, HOPLESS_QUANTIFIER_MESSAGE);
    }

    for (auto [hop, hopNode] : body->getElementChain()) {
        if (hop->getQuantifiedPath()) {
            const bool quantifiesOneRelationshipTwice = hopCount == 1 && !hop->getHopNode(0);
            throwSemanticError(bodyLocation, quantifiesOneRelationshipTwice ? QUANTIFIER_TWICE_MESSAGE : NESTED_QUANTIFIER_MESSAGE);
        }
    }

    EdgePattern* walk = EdgePattern::create(_ast, quantifier, EdgePattern::Direction::Undirected);
    _sourceManager->setLocation(walk, bodyLocation);

    for (auto [hop, hopNode] : body->getElementChain()) {
        walk->addHop(hop);
    }

    for (size_t position = 0; position < entities.size(); position += 2) {
        walk->addHopNode(static_cast<NodePattern*>(entities[position]));
    }

    walk->setHopWhere(where);

    return std::make_pair(walk, node);
}

EdgePattern* CypherGrammar::parseEdgePattern() {
    const SourcePosition start = currentPosition();

    const bool undirected = check(CypherTokenKind::SUB)
                            && peek(1)._kind == CypherTokenKind::SUB
                            && peek(2)._kind != CypherTokenKind::GT;
    if (undirected) {
        consume();
        addPending(CypherTokenKind::GT);
        consume();

        EdgePattern* edge = EdgePattern::create(_ast, nullptr, EdgePattern::Direction::Undirected);
        locate(edge, start);

        return edge;
    }

    return parseExprEdgePattern();
}

EdgePattern* CypherGrammar::parseExprEdgePattern() {
    const SourcePosition start = currentPosition();

    const bool tipFirst = accept(CypherTokenKind::LT);
    expect(CypherTokenKind::SUB);

    if (accept(CypherTokenKind::SUB)) {
        EdgePattern::Direction direction = EdgePattern::Direction::Backward;
        if (!tipFirst) {
            expect(CypherTokenKind::GT);
            direction = EdgePattern::Direction::Forward;
        } else if (accept(CypherTokenKind::GT)) {
            direction = EdgePattern::Direction::Undirected;
        }

        EdgePattern* edge = EdgePattern::create(_ast, nullptr, direction);
        locate(edge, start);

        return edge;
    }

    expect(CypherTokenKind::OBRACK);
    EdgePattern* edge = parseEdgeDetail();
    expect(CypherTokenKind::CBRACK);
    expect(CypherTokenKind::SUB);
    const bool tipLast = accept(CypherTokenKind::GT);

    EdgePattern::Direction direction = EdgePattern::Direction::Undirected;
    if (tipFirst && !tipLast) {
        direction = EdgePattern::Direction::Backward;
    } else if (!tipFirst && tipLast) {
        direction = EdgePattern::Direction::Forward;
    }

    edge->setDirection(direction);
    locate(edge, start);

    return edge;
}

EdgePattern* CypherGrammar::parseEdgeDetail() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::CBRACK)) {
        EdgePattern* edge = ParserUtils::createEdgePattern(_ast, nullptr, nullptr, nullptr, nullptr, nullptr);
        _sourceManager->setLocation(edge, emptyLocation());

        return edge;
    }

    const CypherTokenKind first = examineKind();

    Symbol* symbol = nullptr;
    LabelExpr* labels = nullptr;
    QuantifiedPath* range = nullptr;
    MapLiteral* properties = nullptr;
    WhereClause* where = nullptr;

    if (first == CypherTokenKind::WHERE && whereOpensDetailPredicate(0)) {
        where = parseWhereClause(ExprFollower::NONE);
    } else if (check(CypherTokenKind::COLON) || (first == CypherTokenKind::IS && isOpensDetailLabels(0))) {
        labels = parseLabelExpression();
        range = parseOptionalEdgeRange();
        properties = parseOptionalProperties();
        where = parseOptionalWhereClause();
    } else if (check(CypherTokenKind::MULT)) {
        range = parseEdgeRange();
        properties = parseOptionalProperties();
        where = parseOptionalWhereClause();
    } else if (check(CypherTokenKind::OBRACE)) {
        properties = parseProperties();
        where = parseOptionalWhereClause();
    } else if (checkName()) {
        symbol = parseName();
        labels = parseOptionalLabelExpression();
        range = parseOptionalEdgeRange();
        properties = parseOptionalProperties();
        where = parseOptionalWhereClause();
    } else {
        throwUnexpectedToken();
    }

    EdgePattern* edge = ParserUtils::createEdgePattern(_ast, symbol, labels, range, properties, where);
    locate(edge, start);

    return edge;
}

QuantifiedPath* CypherGrammar::parseOptionalEdgeRange() {
    if (!check(CypherTokenKind::MULT)) {
        return nullptr;
    }

    const SourcePosition start = currentPosition();

    QuantifiedPath* range = parseEdgeRange();
    locate(range, start);

    return range;
}

QuantifiedPath* CypherGrammar::parseEdgeRange() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::MULT);

    QuantifiedPath* range = nullptr;
    if (accept(CypherTokenKind::DIGIT)) {
        const int64_t lower = _previous._integer;

        if (!accept(CypherTokenKind::RANGE)) {
            range = QuantifiedPath::create(_ast);
            range->setLhs(lower);
            range->setRhs(lower);
        } else if (accept(CypherTokenKind::DIGIT)) {
            range = QuantifiedPath::create(_ast);
            range->setLhs(lower);
            range->setRhs(_previous._integer);
        } else {
            range = QuantifiedPath::create(_ast);
            range->setLhs(lower);
        }
    } else if (accept(CypherTokenKind::RANGE)) {
        expect(CypherTokenKind::DIGIT);

        range = QuantifiedPath::create(_ast);
        range->setLhs(1);
        range->setRhs(_previous._integer);
    } else {
        range = QuantifiedPath::create(_ast);
        range->setLhs(1);
    }

    locate(range, start);

    return range;
}

NodePattern* CypherGrammar::parseNodePattern() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OPAREN);

    NodePattern* node = parseNodeDetail();
    expect(CypherTokenKind::CPAREN);

    locate(node, start);

    return node;
}

NodePattern* CypherGrammar::parseNodeDetail() {
    if (check(CypherTokenKind::CPAREN)) {
        return ParserUtils::createNodePattern(_ast, nullptr, nullptr, nullptr, nullptr);
    }

    const CypherTokenKind first = examineKind();

    if (first == CypherTokenKind::WHERE && whereOpensDetailPredicate(0)) {
        WhereClause* where = parseWhereClause(ExprFollower::NONE);

        return ParserUtils::createNodePattern(_ast, nullptr, nullptr, nullptr, where);
    } else if (check(CypherTokenKind::COLON) || (first == CypherTokenKind::IS && isOpensDetailLabels(0))) {
        LabelExpr* labels = parseLabelExpression();
        MapLiteral* properties = parseOptionalProperties();
        WhereClause* where = parseOptionalWhereClause();

        return ParserUtils::createNodePattern(_ast, nullptr, labels, properties, where);
    } else if (check(CypherTokenKind::OBRACE)) {
        MapLiteral* properties = parseProperties();
        WhereClause* where = parseOptionalWhereClause();

        return ParserUtils::createNodePattern(_ast, nullptr, nullptr, properties, where);
    } else if (checkName()) {
        Symbol* name = parseName();
        LabelExpr* labels = parseOptionalLabelExpression();
        MapLiteral* properties = parseOptionalProperties();
        WhereClause* where = parseOptionalWhereClause();

        return ParserUtils::createNodePattern(_ast, name, labels, properties, where);
    }

    throwUnexpectedToken();
}

LabelExpr* CypherGrammar::parseOptionalLabelExpression() {
    if (check(CypherTokenKind::COLON) || check(CypherTokenKind::IS)) {
        return parseLabelExpression();
    }

    return nullptr;
}

MapLiteral* CypherGrammar::parseOptionalProperties() {
    if (check(CypherTokenKind::OBRACE)) {
        return parseProperties();
    }

    return nullptr;
}

MapLiteral* CypherGrammar::parseProperties() {
    return parseMapLiteral();
}

QuantifiedPath* CypherGrammar::parseOptionalQuantifiedPath() {
    if (!startsQuantifiedPath()) {
        return nullptr;
    }

    const SourcePosition start = currentPosition();

    QuantifiedPath* path = parseQuantifiedPath();
    locate(path, start);

    return path;
}

QuantifiedPath* CypherGrammar::parseQuantifiedPath() {
    const SourcePosition start = currentPosition();

    if (accept(CypherTokenKind::PLUS)) {
        QuantifiedPath* path = QuantifiedPath::create(_ast);
        path->setLhs(1);
        locate(path, start);

        return path;
    } else if (accept(CypherTokenKind::MULT)) {
        QuantifiedPath* path = QuantifiedPath::create(_ast);
        locate(path, start);

        return path;
    }

    return parseQuantifiedPathRange();
}

QuantifiedPath* CypherGrammar::parseQuantifiedPathRange() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OBRACE);

    QuantifiedPath* path = nullptr;
    if (accept(CypherTokenKind::COMMA)) {
        if (accept(CypherTokenKind::DIGIT)) {
            const int64_t upper = _previous._integer;
            expect(CypherTokenKind::CBRACE);

            path = QuantifiedPath::create(_ast);
            path->setRhs(upper);
        } else {
            expect(CypherTokenKind::CBRACE);

            path = QuantifiedPath::create(_ast);
        }
    } else {
        expect(CypherTokenKind::DIGIT);
        const int64_t lower = _previous._integer;

        if (!accept(CypherTokenKind::COMMA)) {
            expect(CypherTokenKind::CBRACE);

            path = QuantifiedPath::create(_ast);
            path->setLhs(lower);
            path->setRhs(lower);
        } else if (accept(CypherTokenKind::DIGIT)) {
            const int64_t upper = _previous._integer;
            expect(CypherTokenKind::CBRACE);

            path = QuantifiedPath::create(_ast);
            path->setLhs(lower);
            path->setRhs(upper);
        } else {
            expect(CypherTokenKind::CBRACE);

            path = QuantifiedPath::create(_ast);
            path->setLhs(lower);
        }
    }

    locate(path, start);

    return path;
}

LabelExpr* CypherGrammar::parseLabelExpression() {
    if (!check(CypherTokenKind::COLON) && !check(CypherTokenKind::IS)) {
        throwUnexpectedToken();
    }

    consume();

    return parseLabelDisjunction();
}

LabelExpr* CypherGrammar::parseLabelDisjunction() {
    LabelExpr* disjunction = parseLabelConjunction();

    while (!isFollowedBy(ExprFollower::PIPE) && accept(CypherTokenKind::PIPE)) {
        accept(CypherTokenKind::COLON);

        LabelExpr* operand = parseLabelConjunction();
        disjunction = LabelExpr::createOr(_ast, disjunction, operand);
    }

    return disjunction;
}

LabelExpr* CypherGrammar::parseLabelConjunction() {
    LabelExpr* conjunction = parseLabelNegation();

    while (accept(CypherTokenKind::AMPERSAND) || accept(CypherTokenKind::COLON)) {
        LabelExpr* operand = parseLabelNegation();
        conjunction = LabelExpr::createAnd(_ast, conjunction, operand);
    }

    return conjunction;
}

LabelExpr* CypherGrammar::parseLabelNegation() {
    size_t negations = 0;
    while (accept(CypherTokenKind::EXCLAMATION)) {
        negations++;
    }

    LabelExpr* negation = parseLabelAtom();

    for (size_t index = 0; index < negations; index++) {
        negation = LabelExpr::createNot(_ast, negation);
    }

    return negation;
}

LabelExpr* CypherGrammar::parseLabelAtom() {
    if (accept(CypherTokenKind::OPAREN)) {
        LabelExpr* disjunction = parseLabelDisjunction();
        expect(CypherTokenKind::CPAREN);

        return disjunction;
    } else if (check(CypherTokenKind::DOLLAR)) {
        Parameter* parameter = parseParameter();

        SymbolChain* labels = SymbolChain::create(_ast);
        labels->addParameter(parameter);

        return LabelExpr::createLabels(_ast, labels);
    }

    Symbol* name = parseName();

    SymbolChain* labels = SymbolChain::create(_ast);
    labels->add(name);

    return LabelExpr::createLabels(_ast, labels);
}

std::pair<EdgePattern*, NodePattern*> CypherGrammar::parseExprPatternElemChain() {
    const SourcePosition edgeStart = currentPosition();
    EdgePattern* edge = parseExprEdgePattern();
    const SourceLocation edgeLocation = locationFrom(edgeStart);

    const SourcePosition quantifierStart = currentPosition();
    QuantifiedPath* quantifier = parseOptionalQuantifiedPath();
    const SourceLocation quantifierLocation = quantifier ? locationFrom(quantifierStart) : SourceLocation {};

    NodePattern* node = parseNodePattern();

    if (quantifier) {
        if (edge->getQuantifiedPath()) {
            throwSemanticError(quantifierLocation, QUANTIFIER_TWICE_MESSAGE);
        }

        edge->setQuantifiedPath(quantifier);
    }

    return std::make_pair(ParserUtils::createQuantifiedWalk(_ast, edge, edgeLocation), node);
}

PatternElement* CypherGrammar::parsePathExprElem() {
    const SourcePosition start = currentPosition();

    const std::pair<EdgePattern*, NodePattern*> first = parseExprPatternElemChain();

    PatternElement* element = PatternElement::create(_ast);
    element->addEntity(first.first);
    element->addEntity(first.second);
    locate(element, start);

    while (startsExprPatternElemChain()) {
        const std::pair<EdgePattern*, NodePattern*> hop = parseExprPatternElemChain();
        element->addEntity(hop.first);
        element->addEntity(hop.second);
    }

    return element;
}

Expr* CypherGrammar::parsePathExpr() {
    const SourceLocation opening = currentLocation();
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OPAREN);

    Expr* inner = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::CPAREN);

    if (!startsExprPatternElemChain()) {
        inner->setParenthesized();
        return inner;
    }

    PatternElement* element = parsePathExprElem();

    NodePattern* root = NodePattern::fromExpr(_ast, inner);
    if (!root) {
        throwSemanticError(opening, "Invalid path expr. Root must be a valid node pattern '(symbol? nodeLabels? properties?)'");
        return inner;
    }

    element->addRootEntity(root);
    locate(root, start);

    return ParserUtils::createPatternPredicate(_ast, element, locationFrom(start));
}

Expr* CypherGrammar::parsePathExpr(NodePattern* root, const SourcePosition& rootStart) {
    PatternElement* element = parsePathExprElem();
    element->addRootEntity(root);

    return ParserUtils::createPatternPredicate(_ast, element, locationFrom(rootStart));
}

NodePattern* CypherGrammar::parsePredicateRoot() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::CPAREN)) {
        NodePattern* root = ParserUtils::createNodePattern(_ast, nullptr, nullptr, nullptr, nullptr);
        _sourceManager->setLocation(root, emptyLocation());

        return root;
    }

    const CypherTokenKind first = examineKind();

    NodePattern* root = nullptr;
    if (first == CypherTokenKind::WHERE && whereOpensPredicate(0)) {
        WhereClause* where = parseWhereClause(ExprFollower::NONE);
        root = ParserUtils::createNodePattern(_ast, nullptr, nullptr, nullptr, where);
    } else if (check(CypherTokenKind::COLON) || (first == CypherTokenKind::IS && isOpensPredicateLabels(0))) {
        LabelExpr* labels = parseLabelExpression();
        MapLiteral* properties = parseOptionalProperties();
        WhereClause* where = parseOptionalWhereClause();
        root = ParserUtils::createNodePattern(_ast, nullptr, labels, properties, where);
    } else if (check(CypherTokenKind::OBRACE)) {
        MapLiteral* properties = parseProperties();
        WhereClause* where = parseWhereClause(ExprFollower::NONE);
        root = ParserUtils::createNodePattern(_ast, nullptr, nullptr, properties, where);
    } else {
        Symbol* symbol = parseSymbol();

        if (check(CypherTokenKind::WHERE)) {
            WhereClause* where = parseWhereClause(ExprFollower::NONE);
            root = ParserUtils::createNodePattern(_ast, symbol, nullptr, nullptr, where);
        } else if (check(CypherTokenKind::OBRACE)) {
            const SourcePosition bodyStart = currentPosition();
            MapProjectionExpr::Items items;
            parseMapProjectionBody(items);
            const SourceLocation bodyLocation = locationFrom(bodyStart);

            WhereClause* where = parseWhereClause(ExprFollower::NONE);

            MapLiteral* properties = NodePattern::createProperties(_ast, items);
            if (!properties) {
                throwSemanticError(bodyLocation, "Invalid node pattern. Its properties are 'key: value' entries");
            }

            root = ParserUtils::createNodePattern(_ast, symbol, nullptr, properties, where);
        } else {
            LabelExpr* labels = parseLabelExpression();

            if (check(CypherTokenKind::WHERE)) {
                WhereClause* where = parseWhereClause(ExprFollower::NONE);
                root = ParserUtils::createNodePattern(_ast, symbol, labels, nullptr, where);
            } else {
                MapLiteral* properties = parseProperties();
                WhereClause* where = parseOptionalWhereClause();
                root = ParserUtils::createNodePattern(_ast, symbol, labels, properties, where);
            }
        }
    }

    locate(root, start);

    return root;
}

bool CypherGrammar::startsPatternElemChain() {
    return check(CypherTokenKind::OPAREN)
           || check(CypherTokenKind::SUB)
           || check(CypherTokenKind::LT);
}

// A relationship in an expression can be mistaken for arithmetic: `(x) - [y] - (z)` reads one,
// `(x) - [y + 1] - (z)` subtracts. This peeks at the shape of exprPatternElemChain.
bool CypherGrammar::startsExprPatternElemChain() {
    if (!check(CypherTokenKind::LT) && !check(CypherTokenKind::SUB)) {
        return false;
    }

    const size_t afterEdge = skipExprEdgePattern(0);
    if (afterEdge == 0) {
        return false;
    }

    const size_t afterQuantifier = skipQuantifiedPath(afterEdge);
    if (afterQuantifier == 0 || peek(afterQuantifier)._kind != CypherTokenKind::OPAREN) {
        return false;
    }

    return skipEntityDetail(afterQuantifier + 1, CypherTokenKind::CPAREN) != 0;
}

bool CypherGrammar::startsQuantifiedPath() {
    return check(CypherTokenKind::PLUS)
           || check(CypherTokenKind::MULT)
           || check(CypherTokenKind::OBRACE);
}

size_t CypherGrammar::skipExprEdgePattern(size_t offset) {
    size_t position = offset;

    const bool tipFirst = peek(position)._kind == CypherTokenKind::LT;
    if (tipFirst) {
        position++;
    }

    if (peek(position)._kind != CypherTokenKind::SUB) {
        return 0;
    }

    position++;

    const CypherTokenKind second = peek(position)._kind;
    if (second == CypherTokenKind::SUB) {
        position++;

        if (peek(position)._kind == CypherTokenKind::GT) {
            return position + 1;
        }

        return tipFirst ? position : 0;
    } else if (second != CypherTokenKind::OBRACK) {
        return 0;
    }

    position = skipEntityDetail(position + 1, CypherTokenKind::CBRACK);
    if (position == 0 || peek(position)._kind != CypherTokenKind::SUB) {
        return 0;
    }

    position++;

    if (peek(position)._kind == CypherTokenKind::GT) {
        position++;
    }

    return position;
}

size_t CypherGrammar::skipQuantifiedPath(size_t offset) {
    const CypherTokenKind first = peek(offset)._kind;

    if (first == CypherTokenKind::PLUS || first == CypherTokenKind::MULT) {
        return offset + 1;
    } else if (first != CypherTokenKind::OBRACE) {
        return offset;
    }

    size_t position = offset + 1;

    const bool lower = peek(position)._kind == CypherTokenKind::DIGIT;
    if (lower) {
        position++;
    }

    const bool comma = peek(position)._kind == CypherTokenKind::COMMA;
    if (comma) {
        position++;
    }

    if (comma && peek(position)._kind == CypherTokenKind::DIGIT) {
        position++;
    }

    if ((!lower && !comma) || peek(position)._kind != CypherTokenKind::CBRACE) {
        return 0;
    }

    return position + 1;
}

size_t CypherGrammar::skipEntityDetail(size_t offset, CypherTokenKind closer) {
    size_t position = offset;
    CypherTokenKind kind = peek(position)._kind;

    if (kind == CypherTokenKind::WHERE && whereOpensDetailPredicate(position)) {
        return skipToCloser(position + 1, closer);
    }

    const bool opensLabels = kind == CypherTokenKind::COLON
                             || (kind == CypherTokenKind::IS && isOpensDetailLabels(position));
    if (!opensLabels && CypherToken::isName(kind)) {
        position++;
        kind = peek(position)._kind;
    }

    if (kind == CypherTokenKind::COLON || kind == CypherTokenKind::IS) {
        position = skipLabelExpression(position);
        if (position == 0) {
            return 0;
        }

        kind = peek(position)._kind;
    }

    if (closer == CypherTokenKind::CBRACK && kind == CypherTokenKind::MULT) {
        position = skipEdgeRange(position);
        kind = peek(position)._kind;
    }

    if (kind == CypherTokenKind::OBRACE) {
        position = skipBalanced(position);
        if (position == 0) {
            return 0;
        }

        kind = peek(position)._kind;
    }

    if (kind == CypherTokenKind::WHERE) {
        return skipToCloser(position + 1, closer);
    }

    return kind == closer ? position + 1 : 0;
}

size_t CypherGrammar::skipEdgeRange(size_t offset) {
    size_t position = offset + 1;

    if (peek(position)._kind == CypherTokenKind::DIGIT) {
        position++;
    }

    if (peek(position)._kind == CypherTokenKind::RANGE) {
        position++;

        if (peek(position)._kind == CypherTokenKind::DIGIT) {
            position++;
        }
    }

    return position;
}

size_t CypherGrammar::skipToCloser(size_t offset, CypherTokenKind closer) {
    std::vector<CypherTokenKind> closers;

    for (size_t position = offset;; position++) {
        const CypherTokenKind kind = peek(position)._kind;

        if (closers.empty() && kind == closer) {
            return position + 1;
        } else if (kind == CypherTokenKind::OPAREN) {
            closers.push_back(CypherTokenKind::CPAREN);
        } else if (kind == CypherTokenKind::OBRACK) {
            closers.push_back(CypherTokenKind::CBRACK);
        } else if (kind == CypherTokenKind::OBRACE) {
            closers.push_back(CypherTokenKind::CBRACE);
        } else if (kind == CypherTokenKind::CPAREN || kind == CypherTokenKind::CBRACK || kind == CypherTokenKind::CBRACE) {
            if (closers.empty() || closers.back() != kind) {
                return 0;
            }

            closers.pop_back();
        } else if (kind == CypherTokenKind::PROG_END || kind == CypherTokenKind::LEXER_ERROR) {
            return 0;
        }
    }
}
