#include "CypherGrammar.h"

#include "CypherAST.h"
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

enum class AtomicValue : uint8_t {
    SYMBOL,
    ENTITY_TYPE,
    PROPERTY,
    LOOKUP,
    LABEL_TEST,
};

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
