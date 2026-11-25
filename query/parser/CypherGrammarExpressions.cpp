#include "CypherGrammar.h"

#include <limits>

#include "CypherLexer.h"
#include "Literal.h"
#include "SourceManager.h"
#include "expr/All.h"

using namespace db;

namespace {

constexpr std::string_view IS_OPERAND_MESSAGE = "The right side of IS [NOT] must be NULL or [NFC | NFD | NFKC | NFKD] NORMALIZED";

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
