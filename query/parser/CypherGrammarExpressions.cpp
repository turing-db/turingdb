#include "CypherGrammar.h"

#include <limits>

#include "CypherAST.h"
#include "CypherLexer.h"
#include "FunctionInvocation.h"
#include "Literal.h"
#include "NodePattern.h"
#include "PatternElement.h"
#include "QualifiedName.h"
#include "RegularQuery.h"
#include "SinglePartQuery.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "WhereClause.h"
#include "expr/All.h"

using namespace db;

namespace {

constexpr std::string_view IS_OPERAND_MESSAGE = "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED";

enum class AtomicValue : uint8_t {
    SYMBOL,
    ENTITY_TYPE,
    PROPERTY,
    LOOKUP,
    LABEL_TEST,
};

}

// Each bracket and CASE adds one level of recursion. MAX_NESTING_DEPTH (256) levels must
// fit in half of a 512 KB macOS thread stack. A parser with one function per operator
// level uses 12 stack frames per parenthesis and 483 KB for 256 parentheses. parseExpr
// keeps operators and operands in vectors, so only brackets and CASE grow the stack.
Expr* CypherGrammar::parseExpr(ExprFollower follower) {
    const ExprFollower outerFollower = _follower;
    const size_t outerFollowerDepth = _followerDepth;
    _follower = follower;
    _followerDepth = _depth;

    const size_t operatorBase = _operators.size();
    parseOperand(operatorBase);

    OperatorLevel tightest = OperatorLevel::POWER;
    for (OperatorLevel level = findBinaryOperator(tightest); level != OperatorLevel::NONE; level = findBinaryOperator(tightest)) {
        reduceOperators(operatorBase, level);

        if (accept(CypherTokenKind::IS_VALUE_TEST)) {
            applyIsTest();
            tightest = OperatorLevel::PREDICATE;
            continue;
        }

        pushBinaryOperator(level);
        parseOperand(operatorBase);
        tightest = OperatorLevel::POWER;
    }

    reduceOperators(operatorBase, OperatorLevel::OR);
    Expr* expr = popOperand();

    _follower = outerFollower;
    _followerDepth = outerFollowerDepth;

    return expr;
}

void CypherGrammar::parseOperand(size_t operatorBase) {
    if (pushPrefixes(operatorBase)) {
        return;
    }

    const SourcePosition start = currentPosition();
    Expr* atomic = parseAtomicExpr();

    pushOperand(atomic, start, operatorBase);
}

bool CypherGrammar::pushPrefixes(size_t operatorBase) {
    while (startsNegation(operatorBase)) {
        PendingOperator& negation = _operators.emplace_back();
        negation._kind = CypherTokenKind::NOT;
        negation._level = OperatorLevel::NOT;
        negation._start = currentPosition();

        consume();
    }

    while (check(CypherTokenKind::PLUS) || check(CypherTokenKind::SUB)) {
        const CypherTokenKind sign = examineKind();
        const SourcePosition start = currentPosition();
        consume();

        if (sign == CypherTokenKind::SUB && accept(CypherTokenKind::INT64_MIN_MAGNITUDE)) {
            Expr* literal = LiteralExpr::create(_ast, IntegerLiteral::create(_ast, std::numeric_limits<int64_t>::min()));
            locate(literal, start);

            pushOperand(literal, start, operatorBase);

            return true;
        }

        PendingOperator& pending = _operators.emplace_back();
        pending._kind = sign;
        pending._level = OperatorLevel::SIGN;
        pending._start = start;
    }

    return false;
}

void CypherGrammar::pushOperand(Expr* expr, const SourcePosition& start, size_t operatorBase) {
    PendingOperand& operand = _operands.emplace_back();
    operand._expr = expr;
    operand._start = start;

    reduceOperators(operatorBase, OperatorLevel::SIGN);
}

Expr* CypherGrammar::popOperand() {
    Expr* expr = getExpr(_operands.back());
    _operands.pop_back();

    return expr;
}

bool CypherGrammar::startsNegation(size_t operatorBase) {
    if (examineKind() != CypherTokenKind::NOT) {
        return false;
    } else if (_operators.size() > operatorBase) {
        const OperatorLevel previous = _operators.back()._level;
        const bool takesNegation = previous == OperatorLevel::OR
                                   || previous == OperatorLevel::XOR
                                   || previous == OperatorLevel::AND
                                   || previous == OperatorLevel::NOT;
        if (!takesNegation) {
            return false;
        }
    }

    return notOpensNegation(0, isFollowedBy(ExprFollower::AS));
}

CypherGrammar::OperatorLevel CypherGrammar::findBinaryOperator(OperatorLevel tightest) {
    const bool takesArithmetic = tightest > OperatorLevel::PREDICATE;

    if (takesArithmetic && check(CypherTokenKind::CARET)) {
        return OperatorLevel::POWER;
    } else if (takesArithmetic && (check(CypherTokenKind::MULT) || check(CypherTokenKind::DIV) || check(CypherTokenKind::MOD))) {
        return OperatorLevel::MULT_DIV;
    } else if (takesArithmetic && (check(CypherTokenKind::PLUS) || check(CypherTokenKind::SUB) || check(CypherTokenKind::CONCAT))) {
        return OperatorLevel::ADD_SUB;
    } else if (startsPredicate()) {
        return OperatorLevel::PREDICATE;
    } else if (startsComparison()) {
        return OperatorLevel::COMPARISON;
    } else if (check(CypherTokenKind::AND)) {
        return OperatorLevel::AND;
    } else if (check(CypherTokenKind::XOR)) {
        return OperatorLevel::XOR;
    } else if (check(CypherTokenKind::OR)) {
        return OperatorLevel::OR;
    }

    return OperatorLevel::NONE;
}

void CypherGrammar::pushBinaryOperator(OperatorLevel level) {
    PendingOperator pending {._kind = examineKind(), ._level = level};

    if (level == OperatorLevel::COMPARISON) {
        pending._comparison = parseComparisonSign();
    } else if (level == OperatorLevel::PREDICATE && pending._kind != CypherTokenKind::IN) {
        pending._stringOperator = parseStringExprPrefix();
    } else {
        consume();
    }

    _operators.push_back(pending);
}

void CypherGrammar::reduceOperators(size_t operatorBase, OperatorLevel level) {
    while (_operators.size() > operatorBase && _operators.back()._level >= level) {
        const PendingOperator pending = _operators.back();
        _operators.pop_back();

        if (pending._level == OperatorLevel::SIGN || pending._level == OperatorLevel::NOT) {
            reducePrefix(pending);
        } else {
            reduceBinary(pending);
        }
    }
}

void CypherGrammar::reducePrefix(const PendingOperator& pending) {
    PendingOperand& operand = _operands.back();
    Expr* subExpr = getExpr(operand);

    Expr* expr = nullptr;
    if (pending._level == OperatorLevel::NOT) {
        expr = UnaryExpr::create(_ast, UnaryOperator::Not, subExpr);
    } else if (pending._kind == CypherTokenKind::SUB) {
        expr = ParserUtils::createNegation(_ast, subExpr);
    } else {
        expr = UnaryExpr::create(_ast, UnaryOperator::Plus, subExpr);
    }

    locate(expr, pending._start);

    operand._expr = expr;
    operand._chained = false;
    operand._start = pending._start;
}

void CypherGrammar::reduceBinary(const PendingOperator& pending) {
    const PendingOperand right = _operands.back();
    _operands.pop_back();

    PendingOperand& left = _operands.back();
    Expr* leftExpr = getExpr(left);
    Expr* rightExpr = getExpr(right);
    const SourceLocation location = locationFrom(left._start);

    if (pending._level == OperatorLevel::COMPARISON) {
        const SourceLocation rightLocation = locationFrom(right._start);

        if (left._chained) {
            ParserUtils::extendComparisonChain(_ast, left._chain, pending._comparison, rightExpr, rightLocation, location);
        } else {
            ParserUtils::startComparisonChain(_ast, left._chain, leftExpr, pending._comparison, rightExpr, rightLocation, location);
            left._chained = true;
        }

        return;
    }

    Expr* expr = nullptr;
    switch (pending._kind) {
        case CypherTokenKind::OR:
            expr = LogicalExpr::append(_ast, LogicalOperator::Or, leftExpr, rightExpr);
        break;
        case CypherTokenKind::XOR:
            expr = LogicalExpr::append(_ast, LogicalOperator::Xor, leftExpr, rightExpr);
        break;
        case CypherTokenKind::AND:
            expr = LogicalExpr::append(_ast, LogicalOperator::And, leftExpr, rightExpr);
        break;
        case CypherTokenKind::SUB:
            expr = ParserUtils::createSubtraction(_ast, leftExpr, rightExpr, location);
        break;
        case CypherTokenKind::PLUS:
            expr = BinaryExpr::create(_ast, BinaryOperator::Add, leftExpr, rightExpr);
        break;
        case CypherTokenKind::CONCAT:
            expr = BinaryExpr::create(_ast, BinaryOperator::Concat, leftExpr, rightExpr);
        break;
        case CypherTokenKind::MULT:
            expr = BinaryExpr::create(_ast, BinaryOperator::Mult, leftExpr, rightExpr);
        break;
        case CypherTokenKind::DIV:
            expr = BinaryExpr::create(_ast, BinaryOperator::Div, leftExpr, rightExpr);
        break;
        case CypherTokenKind::MOD:
            expr = BinaryExpr::create(_ast, BinaryOperator::Mod, leftExpr, rightExpr);
        break;
        case CypherTokenKind::CARET:
            expr = BinaryExpr::create(_ast, BinaryOperator::Pow, leftExpr, rightExpr);
        break;
        case CypherTokenKind::IN:
            expr = BinaryExpr::create(_ast, BinaryOperator::In, leftExpr, rightExpr);
        break;
        default:
            expr = StringExpr::create(_ast, pending._stringOperator, leftExpr, rightExpr);
        break;
    }

    if (pending._kind != CypherTokenKind::SUB) {
        _sourceManager->setLocation(expr, location);
    }

    left._expr = expr;
    left._chained = false;
}

void CypherGrammar::applyIsTest() {
    SourceLocation testLocation;
    const ParserUtils::IsTest test = parseIsTest(testLocation);

    PendingOperand& operand = _operands.back();
    Expr* tested = getExpr(operand);

    operand._expr = ParserUtils::createIsTest(_ast, tested, test, testLocation, locationFrom(operand._start));
    operand._chained = false;
}

BinaryOperator CypherGrammar::parseComparisonSign() {
    if (accept(CypherTokenKind::ASSIGN)) {
        return BinaryOperator::Equal;
    } else if (accept(CypherTokenKind::LE)) {
        return BinaryOperator::LessThanOrEqual;
    } else if (accept(CypherTokenKind::GE)) {
        return BinaryOperator::GreaterThanOrEqual;
    } else if (accept(CypherTokenKind::GT)) {
        return BinaryOperator::GreaterThan;
    } else if (accept(CypherTokenKind::LT)) {
        return BinaryOperator::LessThan;
    }

    expect(CypherTokenKind::NOT_EQUAL);

    return BinaryOperator::NotEqual;
}

ParserUtils::IsTest CypherGrammar::parseIsTest(SourceLocation& testLocation) {
    const SourcePosition start = currentPosition();
    const SourceLocation formlessStart = emptyLocation();

    if (accept(CypherTokenKind::NULL_)) {
        testLocation = locationFrom(start);
        return ParserUtils::IsTest {};
    }

    const bool negated = accept(CypherTokenKind::NOT);
    if (negated && accept(CypherTokenKind::NULL_)) {
        testLocation = locationFrom(start);
        return ParserUtils::IsTest {._negated = true};
    }

    const bool hasForm = check(CypherTokenKind::NFC)
                         || check(CypherTokenKind::NFD)
                         || check(CypherTokenKind::NFKC)
                         || check(CypherTokenKind::NFKD);
    const UnicodeNormalForm form = hasForm ? parseNormalForm() : UnicodeNormalForm::NFC;
    const bool consumedAToken = negated || hasForm;

    if (!check(CypherTokenKind::NORMALIZED)) {
        const SourceLocation operand = currentLocation();
        const SourceLocation location = consumedAToken ? SourceLocation::join(locationFrom(start), operand) : operand;
        _lexer->throwSyntaxError(location, IS_OPERAND_MESSAGE);
    }

    consume();

    testLocation = consumedAToken ? locationFrom(start) : SourceLocation::join(formlessStart, _previous._location);

    return ParserUtils::IsTest {._normalForm = form, ._negated = negated};
}

UnicodeNormalForm CypherGrammar::parseNormalForm() {
    if (accept(CypherTokenKind::NFC)) {
        return UnicodeNormalForm::NFC;
    } else if (accept(CypherTokenKind::NFD)) {
        return UnicodeNormalForm::NFD;
    } else if (accept(CypherTokenKind::NFKC)) {
        return UnicodeNormalForm::NFKC;
    }

    expect(CypherTokenKind::NFKD);

    return UnicodeNormalForm::NFKD;
}

StringOperator CypherGrammar::parseStringExprPrefix() {
    if (accept(CypherTokenKind::STARTS)) {
        expect(CypherTokenKind::WITH);
        return StringOperator::StartsWith;
    } else if (accept(CypherTokenKind::ENDS)) {
        expect(CypherTokenKind::WITH);
        return StringOperator::EndsWith;
    }

    expect(CypherTokenKind::CONTAINS);

    return StringOperator::Contains;
}

Expr* CypherGrammar::parseAtomicExpr() {
    const SourcePosition start = currentPosition();

    examineKind();
    const bool opensKeywordConstruct = opensConstruct(0, isFollowedBy(ExprFollower::AS));

    Expr* value = nullptr;
    AtomicValue kind = AtomicValue::LOOKUP;

    if (!opensKeywordConstruct && checkSymbol()) {
        Symbol* symbol = parseSymbol();

        if (check(CypherTokenKind::DOT) || check(CypherTokenKind::OPAREN)) {
            QualifiedName* name = parseQualifiedName(symbol);

            if (check(CypherTokenKind::OPAREN)) {
                FunctionInvocation* invocation = parseFunctionInvocation(name, start);
                value = FunctionInvocationExpr::create(_ast, invocation);
                locate(value, start);
            } else {
                value = ParserUtils::createPropertyChain(_ast, name, locationFrom(start));
                kind = AtomicValue::PROPERTY;
            }
        } else if (check(CypherTokenKind::OBRACE)) {
            value = parseMapProjection(symbol, locationFrom(start));
        } else if (check(CypherTokenKind::COLON) || check(CypherTokenKind::IS)) {
            value = parseEntityTypeExpr(symbol, start);
            kind = AtomicValue::ENTITY_TYPE;
        } else {
            value = SymbolExpr::create(_ast, symbol);
            locate(value, start);
            kind = AtomicValue::SYMBOL;
        }
    } else {
        value = parseAtomExpr();
    }

    while (true) {
        const bool looksUp = kind == AtomicValue::LOOKUP && check(CypherTokenKind::DOT);
        const bool testsLabels = kind == AtomicValue::PROPERTY || kind == AtomicValue::LOOKUP;

        if (check(CypherTokenKind::OBRACK) || looksUp) {
            value = parseLookupOperand(value, start);
            kind = AtomicValue::LOOKUP;
        } else if (testsLabels && (check(CypherTokenKind::COLON) || check(CypherTokenKind::IS))) {
            LabelExpr* labels = parseLabelExpression();
            value = ParserUtils::createLabelTest(_ast, value, labels);
            locate(value, start);
            kind = AtomicValue::LABEL_TEST;
        } else {
            return value;
        }
    }
}

Expr* CypherGrammar::parseLookupOperand(Expr* operand, const SourcePosition& operandStart) {
    if (accept(CypherTokenKind::DOT)) {
        Symbol* name = parseName();

        Expr* lookup = ParserUtils::createPropertyLookup(_ast, operand, name);
        locate(lookup, operandStart);

        return lookup;
    }

    expect(CypherTokenKind::OBRACK);

    Expr* lookup = nullptr;
    if (accept(CypherTokenKind::RANGE)) {
        if (accept(CypherTokenKind::CBRACK)) {
            lookup = ListSliceExpr::create(_ast, operand, nullptr, nullptr);
        } else {
            Expr* upper = parseExpr(ExprFollower::NONE);
            expect(CypherTokenKind::CBRACK);

            lookup = ListSliceExpr::create(_ast, operand, nullptr, upper);
        }
    } else {
        Expr* index = parseExpr(ExprFollower::NONE);

        if (!accept(CypherTokenKind::RANGE)) {
            expect(CypherTokenKind::CBRACK);

            lookup = IndexExpr::create(_ast, operand, index);
        } else if (accept(CypherTokenKind::CBRACK)) {
            lookup = ListSliceExpr::create(_ast, operand, index, nullptr);
        } else {
            Expr* upper = parseExpr(ExprFollower::NONE);
            expect(CypherTokenKind::CBRACK);

            lookup = ListSliceExpr::create(_ast, operand, index, upper);
        }
    }

    locate(lookup, operandStart);

    return lookup;
}

EntityTypeExpr* CypherGrammar::parseEntityTypeExpr(Symbol* symbol, const SourcePosition& symbolStart) {
    LabelExpr* labels = parseLabelExpression();

    EntityTypeExpr* entity = EntityTypeExpr::create(_ast, symbol, labels);
    locate(entity, symbolStart);

    return entity;
}

PropertyExpr* CypherGrammar::parsePropertyExpr(QualifiedName* name, const SourcePosition& nameStart) {
    PropertyExpr* property = PropertyExpr::create(_ast, name);
    locate(property, nameStart);

    return property;
}

PropertyExpr* CypherGrammar::parseVariablePropertyExpr() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::OPAREN)) {
        Expr* base = parseParenthesizedExpr();
        const SourceLocation baseLocation = locationFrom(start);

        expect(CypherTokenKind::DOT);
        Symbol* name = parseName();

        PropertyExpr* property = ParserUtils::createParenthesizedPropertyAccess(_ast, base, name);
        if (!property) {
            reportSemanticError(baseLocation, "Invalid property access. Only a variable can be parenthesized on the left of '.'");
            return nullptr;
        }

        locate(property, start);

        return property;
    }

    Symbol* symbol = parseSymbol();
    if (!check(CypherTokenKind::DOT)) {
        throwUnexpectedToken();
    }

    QualifiedName* name = parseQualifiedName(symbol);

    return parsePropertyExpr(name, start);
}

QualifiedName* CypherGrammar::parseQualifiedName(Symbol* first) {
    QualifiedName* name = QualifiedName::create(_ast);
    name->addName(first);

    while (accept(CypherTokenKind::DOT)) {
        Symbol* nextName = parseName();
        _sourceManager->setLocation(nextName, _previous._location);

        name->addName(nextName);
    }

    return name;
}

QualifiedName* CypherGrammar::parseInvocationName(Symbol* first) {
    return parseQualifiedName(first);
}

Expr* CypherGrammar::parseAtomExpr() {
    if (check(CypherTokenKind::OPAREN)) {
        const ParenthesizedAtom atom = classifyParenthesis();

        if (atom == ParenthesizedAtom::EXPRESSION) {
            return parsePathExpr();
        } else if (atom == ParenthesizedAtom::EMBEDDING) {
            return parseLiteralExpr();
        }

        return parsePatternPredicate();
    } else if (check(CypherTokenKind::DOLLAR)) {
        return parseParameterExpr();
    } else if (check(CypherTokenKind::CASE)) {
        return parseCaseExpr();
    } else if (check(CypherTokenKind::COUNT) && peek(1)._kind == CypherTokenKind::OBRACE) {
        return parseSubqueryCount();
    } else if (check(CypherTokenKind::COUNT) || check(CypherTokenKind::NORMALIZE)) {
        return parseKeywordInvocationExpr();
    } else if (check(CypherTokenKind::OBRACK)) {
        return parseBracketExpr();
    } else if (check(CypherTokenKind::ALL)
               || check(CypherTokenKind::ANY)
               || check(CypherTokenKind::NONE)
               || check(CypherTokenKind::SINGLE)) {
        return parseFilterWith();
    } else if (check(CypherTokenKind::REDUCE)) {
        return parseReduceExpr();
    } else if (check(CypherTokenKind::ALLREDUCE)) {
        return parseAllReduceExpr();
    } else if (check(CypherTokenKind::EXISTS)) {
        return parseSubqueryExist();
    } else if (check(CypherTokenKind::COLLECT)) {
        return parseCollectExpr();
    }

    const bool startsLiteral = check(CypherTokenKind::TRUE)
                               || check(CypherTokenKind::FALSE)
                               || check(CypherTokenKind::NULL_)
                               || check(CypherTokenKind::DIGIT)
                               || check(CypherTokenKind::DOUBLE)
                               || check(CypherTokenKind::STRING_LITERAL)
                               || check(CypherTokenKind::OBRACE);
    if (!startsLiteral) {
        throwUnexpectedToken();
    }

    return parseLiteralExpr();
}

Expr* CypherGrammar::parsePatternPredicate() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OPAREN);

    NodePattern* root = parsePredicateRoot();
    expect(CypherTokenKind::CPAREN);

    const SourceLocation rootLocation = locationFrom(start);

    if (startsExprPatternElemChain()) {
        return parsePathExpr(root, start);
    }

    PatternElement* element = PatternElement::create(_ast);
    element->addEntity(root);

    return ParserUtils::createPatternPredicate(_ast, element, rootLocation);
}

Expr* CypherGrammar::parseKeywordInvocationExpr() {
    const SourcePosition start = currentPosition();
    FunctionInvocation* invocation = examineKind() == CypherTokenKind::COUNT ? parseCountFunc() : parseNormalizeFunc();

    Expr* expr = FunctionInvocationExpr::create(_ast, invocation);
    locate(expr, start);

    return expr;
}

Expr* CypherGrammar::parseParenthesizedExpr() {
    expect(CypherTokenKind::OPAREN);

    Expr* expr = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::CPAREN);

    expr->setParenthesized();

    return expr;
}

FunctionInvocation* CypherGrammar::parseFunctionInvocation(QualifiedName* name, const SourcePosition& nameStart) {
    expect(CypherTokenKind::OPAREN);

    if (accept(CypherTokenKind::CPAREN)) {
        FunctionInvocation* invocation = FunctionInvocation::create(_ast, name);
        invocation->setArguments(ExprChain::create(_ast));
        locate(invocation, nameStart);

        return invocation;
    }

    const bool distinct = examineKind() == CypherTokenKind::DISTINCT && distinctOpensArgument(true);
    if (distinct) {
        consume();

        if (accept(CypherTokenKind::CPAREN)) {
            reportSemanticError(locationFrom(nameStart), "DISTINCT requires an argument");
            return nullptr;
        }
    }

    ExprChain* arguments = parseExprChain();
    expect(CypherTokenKind::CPAREN);

    FunctionInvocation* invocation = FunctionInvocation::create(_ast, name);
    invocation->setArguments(arguments);

    if (distinct) {
        invocation->setDistinct(true);
    }

    locate(invocation, nameStart);

    return invocation;
}

FunctionInvocation* CypherGrammar::parseCountFunc() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::COUNT);
    expect(CypherTokenKind::OPAREN);

    if (accept(CypherTokenKind::MULT)) {
        expect(CypherTokenKind::CPAREN);

        ExprChain* arguments = ExprChain::create(_ast);
        arguments->add(LiteralExpr::create(_ast, WildcardLiteral::create(_ast)));

        FunctionInvocation* invocation = ParserUtils::createInvocation(_ast, "count", arguments);
        locate(invocation, start);

        return invocation;
    } else if (accept(CypherTokenKind::CPAREN)) {
        reportNotImplemented(locationFrom(start), "count()");
        return nullptr;
    }

    const bool distinct = examineKind() == CypherTokenKind::DISTINCT && distinctOpensArgument(true);
    if (distinct) {
        consume();

        if (accept(CypherTokenKind::CPAREN)) {
            reportNotImplemented(locationFrom(start), "count(DISTINCT)");
            return nullptr;
        }
    }

    ExprChain* arguments = parseExprChain();
    expect(CypherTokenKind::CPAREN);

    FunctionInvocation* invocation = ParserUtils::createInvocation(_ast, "count", arguments);

    if (distinct) {
        invocation->setDistinct(true);
    }

    locate(invocation, start);

    return invocation;
}

FunctionInvocation* CypherGrammar::parseNormalizeFunc() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::NORMALIZE);
    expect(CypherTokenKind::OPAREN);

    Expr* string = parseExpr(ExprFollower::NONE);

    UnicodeNormalForm form = UnicodeNormalForm::NFC;
    if (accept(CypherTokenKind::COMMA)) {
        form = parseNormalForm();
    }

    expect(CypherTokenKind::CPAREN);

    FunctionInvocation* invocation = ParserUtils::createNormalizeInvocation(_ast, string, form);
    locate(invocation, start);

    return invocation;
}

Expr* CypherGrammar::parseCollectExpr() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::COLLECT);

    if (accept(CypherTokenKind::OBRACE)) {
        RegularQuery* body = parseQueryBody();
        expect(CypherTokenKind::CBRACE);

        Expr* collect = CollectSubqueryExpr::create(_ast, body);
        locate(collect, start);

        return collect;
    }

    expect(CypherTokenKind::OPAREN);

    const bool distinct = examineKind() == CypherTokenKind::DISTINCT && distinctOpensArgument(false);
    if (distinct) {
        consume();
    }

    Expr* argument = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::CPAREN);

    ExprChain* arguments = ExprChain::create(_ast);
    arguments->add(argument);

    FunctionInvocation* invocation = ParserUtils::createInvocation(_ast, "collect", arguments);

    if (distinct) {
        invocation->setDistinct(true);
    }

    Expr* collect = FunctionInvocationExpr::create(_ast, invocation);
    locate(collect, start);

    return collect;
}

Expr* CypherGrammar::parseSubqueryExist() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::EXISTS);

    if (check(CypherTokenKind::OPAREN)) {
        return parseExistsPredicate(start);
    }

    expect(CypherTokenKind::OBRACE);
    RegularQuery* body = readsBracedPattern() ? parsePatternBody() : parseQueryBody();
    expect(CypherTokenKind::CBRACE);

    Expr* exists = ExistsExpr::create(_ast, body);
    locate(exists, start);

    return exists;
}

Expr* CypherGrammar::parseExistsPredicate(const SourcePosition& start) {
    expect(CypherTokenKind::OPAREN);

    const SourcePosition argumentStart = currentPosition();
    Expr* argument = parseExpr(ExprFollower::NONE);
    const SourceLocation argumentLocation = locationFrom(argumentStart);
    expect(CypherTokenKind::CPAREN);

    ExistsExpr* exists = ParserUtils::findPatternPredicate(argument);
    if (!exists) {
        reportSemanticError(argumentLocation, "exists() takes a pattern: test a property with IS NOT NULL");
        return argument;
    }

    locate(exists, start);

    return exists;
}

Expr* CypherGrammar::parseSubqueryCount() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::COUNT);

    expect(CypherTokenKind::OBRACE);
    RegularQuery* body = readsBracedPattern() ? parsePatternBody() : parseQueryBody();
    expect(CypherTokenKind::CBRACE);

    Expr* count = CountSubqueryExpr::create(_ast, body);
    locate(count, start);

    return count;
}

SinglePartQuery* CypherGrammar::parsePatternBody() {
    const SourcePosition start = currentPosition();
    Pattern* pattern = parsePatternWhere();

    return ParserUtils::createPatternBody(_ast, pattern, locationFrom(start));
}

Expr* CypherGrammar::parseFilterWith() {
    const SourcePosition start = currentPosition();

    ListPredicateExpr::Quantifier quantifier = ListPredicateExpr::Quantifier::Single;
    if (accept(CypherTokenKind::ALL)) {
        quantifier = ListPredicateExpr::Quantifier::All;
    } else if (accept(CypherTokenKind::ANY)) {
        quantifier = ListPredicateExpr::Quantifier::Any;
    } else if (accept(CypherTokenKind::NONE)) {
        quantifier = ListPredicateExpr::Quantifier::None;
    } else {
        expect(CypherTokenKind::SINGLE);
    }

    expect(CypherTokenKind::OPAREN);
    ListComprehensionExpr* filter = parseFilterExpr();
    expect(CypherTokenKind::CPAREN);

    Expr* predicate = ListPredicateExpr::create(_ast, quantifier, filter);
    locate(predicate, start);

    return predicate;
}

ListComprehensionExpr* CypherGrammar::parseFilterExpr() {
    const SourcePosition start = currentPosition();

    Symbol* variable = parseName();
    expect(CypherTokenKind::IN);
    Expr* list = parseExpr(ExprFollower::NONE);

    WhereClause* where = nullptr;
    if (check(CypherTokenKind::WHERE)) {
        where = parseWhereClause(ExprFollower::NONE);
    }

    ListComprehensionExpr* filter = ListComprehensionExpr::create(_ast, variable, list);

    if (where) {
        filter->setPredicate(where->getExpr());
    }

    locate(filter, start);

    return filter;
}

Expr* CypherGrammar::parseReduceExpr() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::REDUCE);
    expect(CypherTokenKind::OPAREN);

    Symbol* accumulator = parseName();
    expect(CypherTokenKind::ASSIGN);
    Expr* initial = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::COMMA);

    Symbol* variable = parseName();
    expect(CypherTokenKind::IN);

    Expr* list = nullptr;
    Expr* step = nullptr;
    parseChoosingBar([this, &list, &step] {
        list = parseExpr(ExprFollower::PIPE);
        expect(CypherTokenKind::PIPE);

        step = parseExpr(ExprFollower::NONE);
        expect(CypherTokenKind::CPAREN);

        return true;
    });

    Expr* reduce = ReduceExpr::create(_ast, accumulator, initial, variable, list, step);
    locate(reduce, start);

    return reduce;
}

Expr* CypherGrammar::parseAllReduceExpr() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::ALLREDUCE);
    expect(CypherTokenKind::OPAREN);

    Symbol* accumulator = parseName();
    expect(CypherTokenKind::ASSIGN);
    Expr* initial = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::COMMA);

    Symbol* variable = parseName();
    expect(CypherTokenKind::IN);

    Expr* list = nullptr;
    Expr* step = nullptr;
    Expr* predicate = nullptr;
    parseChoosingBar([this, &list, &step, &predicate] {
        list = parseExpr(ExprFollower::PIPE);
        expect(CypherTokenKind::PIPE);

        step = parseExpr(ExprFollower::NONE);
        expect(CypherTokenKind::COMMA);
        predicate = parseExpr(ExprFollower::NONE);
        expect(CypherTokenKind::CPAREN);

        return true;
    });

    Expr* reduce = AllReduceExpr::create(_ast, accumulator, initial, variable, list, step, predicate);
    locate(reduce, start);

    return reduce;
}

Expr* CypherGrammar::parseMapProjection(Symbol* symbol, const SourceLocation& symbolLocation) {
    MapProjectionExpr::Items items;
    parseMapProjectionBody(items);

    SymbolExpr* base = SymbolExpr::create(_ast, symbol);
    _sourceManager->setLocation(base, symbolLocation);

    MapProjectionExpr* projection = MapProjectionExpr::create(_ast, base, items);
    _sourceManager->setLocation(projection, SourceLocation::join(symbolLocation, _previous._location));

    return projection;
}

void CypherGrammar::parseMapProjectionBody(MapProjectionExpr::Items& items) {
    expect(CypherTokenKind::OBRACE);

    if (accept(CypherTokenKind::CBRACE)) {
        return;
    }

    parseMapProjectionItems(items);
    expect(CypherTokenKind::CBRACE);
}

void CypherGrammar::parseMapProjectionItems(MapProjectionExpr::Items& items) {
    items.push_back(parseMapProjectionItem());

    while (accept(CypherTokenKind::COMMA)) {
        items.push_back(parseMapProjectionItem());
    }
}

MapProjectionExpr::Item CypherGrammar::parseMapProjectionItem() {
    if (accept(CypherTokenKind::DOT)) {
        if (accept(CypherTokenKind::MULT)) {
            return MapProjectionExpr::Item {._kind = MapProjectionExpr::ItemKind::AllProperties};
        }

        Symbol* key = parseName();

        return MapProjectionExpr::Item {._kind = MapProjectionExpr::ItemKind::Property, ._key = key};
    }

    const SourcePosition start = currentPosition();
    const bool variable = checkSymbol();

    Symbol* key = parseName();

    if (accept(CypherTokenKind::COLON)) {
        Expr* value = parseExpr(ExprFollower::NONE);

        return MapProjectionExpr::Item {._kind = MapProjectionExpr::ItemKind::Literal, ._key = key, ._value = value};
    } else if (!variable) {
        throwUnexpectedToken();
    }

    SymbolExpr* value = SymbolExpr::create(_ast, key);
    locate(value, start);

    return MapProjectionExpr::Item {._kind = MapProjectionExpr::ItemKind::Variable, ._key = key, ._value = value};
}

Expr* CypherGrammar::parseBracketExpr() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OBRACK);

    if (accept(CypherTokenKind::CBRACK)) {
        ListLiteral* list = ListLiteral::create(_ast);
        locate(list, start);

        Expr* expr = LiteralExpr::create(_ast, list);
        locate(expr, start);

        return expr;
    }

    Expr* bracket = nullptr;
    const bool opensListComprehension = CypherToken::isSymbol(peek(0)._kind) && peek(1)._kind == CypherTokenKind::IN;

    if (!opensListComprehension) {
        parseBracketBody(start, ExprFollower::NONE, bracket);
        return bracket;
    }

    parseChoosingBar([this, &start, &bracket] {
        return parseBracketBody(start, ExprFollower::PIPE, bracket);
    });

    return bracket;
}

bool CypherGrammar::parseBracketBody(const SourcePosition& start, ExprFollower follower, Expr*& bracket) {
    const SourcePosition firstStart = currentPosition();
    Expr* first = parseExpr(follower);
    const SourceLocation firstLocation = locationFrom(firstStart);

    if (check(CypherTokenKind::WHERE) || check(CypherTokenKind::PIPE)) {
        const std::pair<WhereClause*, Expr*> tail = parseComprehensionTail();

        bracket = ParserUtils::createComprehension(_ast, first, tail.first, tail.second, firstLocation);
        if (bracket) {
            locate(bracket, start);
        } else {
            reportSemanticError(firstLocation,
                                "Invalid comprehension. Expected 'variable IN list', or a path '(...)-[...]-(...)' followed by '|'");
            bracket = first;
        }

        return tail.second != nullptr;
    }

    ListLiteral* list = parseListLiteralItems(first, firstLocation);
    expect(CypherTokenKind::CBRACK);

    locate(list, start);

    bracket = ParserUtils::createListOrComprehension(_ast, list);
    locate(bracket, start);

    return false;
}

std::pair<WhereClause*, Expr*> CypherGrammar::parseComprehensionTail() {
    if (check(CypherTokenKind::WHERE)) {
        WhereClause* where = nullptr;
        Expr* projection = nullptr;

        parseChoosingBar([this, &where, &projection] {
            where = parseWhereClause(ExprFollower::PIPE);
            projection = accept(CypherTokenKind::PIPE) ? parseExpr(ExprFollower::NONE) : nullptr;
            expect(CypherTokenKind::CBRACK);

            return projection != nullptr;
        });

        return std::make_pair(where, projection);
    }

    expect(CypherTokenKind::PIPE);
    Expr* projection = parseExpr(ExprFollower::NONE);
    expect(CypherTokenKind::CBRACK);

    return std::make_pair(nullptr, projection);
}

ListLiteral* CypherGrammar::parseListLiteralItems(Expr* firstItem, const SourceLocation& firstItemLocation) {
    _sourceManager->setLocation(firstItem, firstItemLocation);

    ListLiteral* list = ListLiteral::create(_ast);
    list->addItem(firstItem);

    while (accept(CypherTokenKind::COMMA)) {
        list->addItem(parseListLiteralItem());
    }

    return list;
}

Expr* CypherGrammar::parseListLiteralItem() {
    const SourcePosition start = currentPosition();

    Expr* item = parseExpr(ExprFollower::NONE);
    locate(item, start);

    return item;
}

CaseExpr* CypherGrammar::parseCaseExpr() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CASE);
    enterNesting(_previous._location);

    Expr* subject = nullptr;
    const bool opensBranch = examineKind() == CypherTokenKind::WHEN && whenOpensBranch();
    if (!opensBranch) {
        subject = parseExpr(ExprFollower::NONE);
    }

    CaseExpr* caseExpr = parseWhenThenChain();

    Expr* elseExpr = nullptr;
    if (accept(CypherTokenKind::ELSE)) {
        elseExpr = parseExpr(ExprFollower::NONE);
    }

    expect(CypherTokenKind::END);
    leaveNesting();

    if (subject) {
        caseExpr->setSubject(subject);
    }

    if (elseExpr) {
        caseExpr->setElseExpr(elseExpr);
    }

    locate(caseExpr, start);

    return caseExpr;
}

CaseExpr* CypherGrammar::parseWhenThenChain() {
    CaseExpr::Tests tests;
    Expr* value = parseWhenThen(tests);

    CaseExpr* caseExpr = CaseExpr::create(_ast);
    caseExpr->addBranch(tests, value);

    while (check(CypherTokenKind::WHEN)) {
        tests.clear();
        value = parseWhenThen(tests);
        caseExpr->addBranch(tests, value);
    }

    return caseExpr;
}

Expr* CypherGrammar::parseWhenThen(CaseExpr::Tests& tests) {
    expect(CypherTokenKind::WHEN);
    parseCaseTests(tests);

    expect(CypherTokenKind::THEN);

    return parseExpr(ExprFollower::NONE);
}

void CypherGrammar::parseCaseTests(CaseExpr::Tests& tests) {
    tests.push_back(parseCaseTest());

    while (accept(CypherTokenKind::COMMA)) {
        tests.push_back(parseCaseTest());
    }
}

CaseExpr::Test CypherGrammar::parseCaseTest() {
    if (accept(CypherTokenKind::IS_VALUE_TEST)) {
        if (accept(CypherTokenKind::NOT)) {
            expect(CypherTokenKind::NULL_);
            return CaseExpr::Test {._kind = CaseExpr::TestKind::IsNotNull};
        }

        expect(CypherTokenKind::NULL_);

        return CaseExpr::Test {._kind = CaseExpr::TestKind::IsNull};
    } else if (startsComparison()) {
        const BinaryOperator comparison = parseComparisonSign();
        Expr* value = parseExpr(ExprFollower::NONE);

        return CaseExpr::Test {._value = value, ._operator = comparison, ._kind = CaseExpr::TestKind::Comparison};
    }

    return CaseExpr::Test {._value = parseExpr(ExprFollower::NONE)};
}

bool CypherGrammar::startsPredicate() {
    return check(CypherTokenKind::STARTS)
           || check(CypherTokenKind::ENDS)
           || check(CypherTokenKind::CONTAINS)
           || check(CypherTokenKind::IN)
           || check(CypherTokenKind::IS_VALUE_TEST);
}

bool CypherGrammar::startsComparison() {
    return check(CypherTokenKind::ASSIGN)
           || check(CypherTokenKind::LE)
           || check(CypherTokenKind::GE)
           || check(CypherTokenKind::GT)
           || check(CypherTokenKind::LT)
           || check(CypherTokenKind::NOT_EQUAL);
}

Expr* CypherGrammar::getExpr(const PendingOperand& operand) {
    return operand._chained ? operand._chain._expr : operand._expr;
}
