#include "CypherGrammar.h"

#include "CypherAST.h"
#include "FunctionInvocation.h"
#include "Literal.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "RegularQuery.h"
#include "SinglePartQuery.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "WhereClause.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "expr/EntityTypeExpr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"
#include "stmt/CallStmt.h"
#include "stmt/CallSubqueryStmt.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"
#include "stmt/Limit.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/MatchStmt.h"
#include "stmt/MergeStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/RemoveStmt.h"
#include "stmt/ReturnStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "stmt/WithStmt.h"

using namespace db;

namespace {

bool opensClause(CypherTokenKind kind) {
    return kind == CypherTokenKind::MATCH
           || kind == CypherTokenKind::OPTIONAL
           || kind == CypherTokenKind::WITH
           || kind == CypherTokenKind::UNWIND
           || kind == CypherTokenKind::CALL
           || kind == CypherTokenKind::LOAD
           || kind == CypherTokenKind::VECTOR
           || kind == CypherTokenKind::CREATE
           || kind == CypherTokenKind::MERGE
           || kind == CypherTokenKind::DELETE
           || kind == CypherTokenKind::DETACH
           || kind == CypherTokenKind::SET
           || kind == CypherTokenKind::REMOVE
           || kind == CypherTokenKind::FOREACH
           || kind == CypherTokenKind::RETURN
           || kind == CypherTokenKind::WHEN;
}

}

SinglePartQuery* CypherGrammar::parseSinglePartQuery() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::RETURN)) {
        ReturnStmt* returnStmt = parseReturnStmt();

        SinglePartQuery* query = SinglePartQuery::create(_ast);
        query->setReturnStmt(returnStmt);
        locate(query, start);

        return query;
    }

    StmtContainer* stmts = parseQueryStatements();

    ShortestPathStmt* shortestPath = nullptr;
    ReturnStmt* returnStmt = nullptr;
    if (check(CypherTokenKind::SHORTESTPATH)) {
        shortestPath = parseShortestPathStmt();
        returnStmt = parseReturnStmt();
    } else if (check(CypherTokenKind::RETURN)) {
        returnStmt = parseReturnStmt();
    }

    SinglePartQuery* query = SinglePartQuery::create(_ast);
    query->setStmts(stmts);

    if (shortestPath) {
        query->addStmt(shortestPath);
    }

    if (returnStmt) {
        query->setReturnStmt(returnStmt);
    }

    locate(query, start);

    return query;
}

StmtContainer* CypherGrammar::parseQueryStatements() {
    const SourcePosition start = currentPosition();

    Stmt* first = parseQueryStatement();

    StmtContainer* stmts = StmtContainer::create(_ast);
    stmts->add(first);
    locate(stmts, start);

    while (startsQueryStatement()) {
        stmts->add(parseQueryStatement());
        locate(stmts, start);
    }

    return stmts;
}

Stmt* CypherGrammar::parseQueryStatement() {
    if (startsReadingStatement()) {
        return parseReadingStatement();
    }

    return parseUpdatingStatement();
}

Stmt* CypherGrammar::parseReadingStatement() {
    if (check(CypherTokenKind::MATCH)) {
        return parseMatchStmt();
    } else if (check(CypherTokenKind::OPTIONAL)) {
        const CypherTokenKind next = peek(1)._kind;

        if (next == CypherTokenKind::MATCH) {
            return parseMatchStmt();
        } else if (next == CypherTokenKind::CALL && opensCallSubquery(2)) {
            return parseCallSubqueryStmt();
        } else if (next == CypherTokenKind::CALL && CypherToken::isSymbol(peek(2)._kind)) {
            return parseCallStmt();
        } else if (next == CypherTokenKind::CALL) {
            throwUnexpectedTokenAt(2, getCallOpeningKinds());
        }

        throwUnexpectedTokenAt(1, kindsOf({CypherTokenKind::MATCH, CypherTokenKind::CALL}));
    } else if (check(CypherTokenKind::WITH)) {
        return parseWithStmt();
    } else if (check(CypherTokenKind::UNWIND)) {
        return parseUnwindStmt();
    } else if (check(CypherTokenKind::CALL)) {
        if (opensCallSubquery(1)) {
            return parseCallSubqueryStmt();
        } else if (CypherToken::isSymbol(peek(1)._kind)) {
            return parseCallStmt();
        }

        throwUnexpectedTokenAt(1, getCallOpeningKinds());
    } else if (check(CypherTokenKind::LOAD)) {
        return parseLoadCSVStmt();
    } else if (check(CypherTokenKind::VECTOR)) {
        return parseVectorSearchStmt();
    }

    throwUnexpectedToken();
}

Stmt* CypherGrammar::parseUpdatingStatement() {
    if (check(CypherTokenKind::CREATE)) {
        return parseCreateStmt();
    } else if (check(CypherTokenKind::MERGE)) {
        return parseMergeStmt();
    } else if (check(CypherTokenKind::DELETE) || check(CypherTokenKind::DETACH)) {
        return parseDeleteStmt();
    } else if (check(CypherTokenKind::SET)) {
        return parseSetStmt();
    } else if (check(CypherTokenKind::REMOVE)) {
        return parseRemoveStmt();
    } else if (check(CypherTokenKind::FOREACH)) {
        return parseForeachStmt();
    }

    throwUnexpectedToken();
}

ReturnStmt* CypherGrammar::parseReturnStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::RETURN);

    Projection* projection = parseProjectionBody();

    ReturnStmt* stmt = ReturnStmt::create(_ast, projection);
    locate(stmt, start);

    return stmt;
}

WithStmt* CypherGrammar::parseWithStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::WITH);

    Projection* projection = parseProjectionBody();
    WhereClause* where = parseOptionalWhereClause();

    WithStmt* stmt = WithStmt::create(_ast, projection);
    stmt->setWhere(where);
    locate(stmt, start);

    return stmt;
}

Projection* CypherGrammar::parseProjectionBody() {
    if (examineKind() == CypherTokenKind::DISTINCT && distinctOpensProjection()) {
        consume();

        Projection* projection = parseProjectionClauses();
        projection->setDistinct(true);

        return projection;
    }

    return parseProjectionClauses();
}

Projection* CypherGrammar::parseProjectionClauses() {
    Projection* projection = parseProjectionItems();
    OrderBy* orderBy = parseOptionalOrderBySubStmt();
    Skip* skip = parseOptionalSkipSubStmt();
    Limit* limit = parseOptionalLimitSubStmt();

    projection->setOrderBy(orderBy);
    projection->setSkip(skip);
    projection->setLimit(limit);

    return projection;
}

Projection* CypherGrammar::parseProjectionItems() {
    const SourcePosition start = currentPosition();

    Projection* projection = nullptr;
    if (accept(CypherTokenKind::MULT)) {
        projection = Projection::create(_ast);
        projection->setReturnAll();
    } else {
        Expr* item = parseProjectionItem();

        projection = Projection::create(_ast);
        projection->addExpr(item);
    }

    locate(projection, start);

    while (accept(CypherTokenKind::COMMA)) {
        projection->addExpr(parseProjectionItem());
        locate(projection, start);
    }

    return projection;
}

Expr* CypherGrammar::parseProjectionItem() {
    const SourcePosition start = currentPosition();

    Expr* expr = parseExpr(ExprFollower::AS);

    if (accept(CypherTokenKind::AS)) {
        Symbol* name = parseName();
        expr->setName(name->getName());
    }

    locate(expr, start);

    return expr;
}

OrderBy* CypherGrammar::parseOptionalOrderBySubStmt() {
    if (check(CypherTokenKind::ORDER)) {
        return parseOrderBySubStmt();
    }

    return nullptr;
}

OrderBy* CypherGrammar::parseOrderBySubStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::ORDER);
    expect(CypherTokenKind::BY);

    OrderByItem* first = parseOrderByItem();

    OrderBy* orderBy = OrderBy::create(_ast);
    orderBy->addItem(first);
    locate(orderBy, start);

    while (accept(CypherTokenKind::COMMA)) {
        orderBy->addItem(parseOrderByItem());
        locate(orderBy, start);
    }

    return orderBy;
}

OrderByItem* CypherGrammar::parseOrderByItem() {
    const SourcePosition start = currentPosition();

    Expr* expr = parseExpr(ExprFollower::NONE);

    OrderByItem* item = nullptr;
    if (accept(CypherTokenKind::ASCENDING) || accept(CypherTokenKind::ASC)) {
        item = OrderByItem::create(_ast, expr, OrderByType::ASC);
    } else if (accept(CypherTokenKind::DESCENDING) || accept(CypherTokenKind::DESC)) {
        item = OrderByItem::create(_ast, expr, OrderByType::DESC);
    } else {
        item = OrderByItem::create(_ast, expr);
    }

    locate(item, start);

    return item;
}

Skip* CypherGrammar::parseOptionalSkipSubStmt() {
    if (check(CypherTokenKind::SKIP)) {
        return parseSkipSubStmt();
    }

    return nullptr;
}

Skip* CypherGrammar::parseSkipSubStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SKIP);

    Expr* count = parseExpr(ExprFollower::NONE);

    Skip* skip = Skip::create(_ast, count);
    locate(skip, start);

    return skip;
}

Limit* CypherGrammar::parseOptionalLimitSubStmt() {
    if (check(CypherTokenKind::LIMIT)) {
        return parseLimitSubStmt();
    }

    return nullptr;
}

Limit* CypherGrammar::parseLimitSubStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LIMIT);

    Expr* count = parseExpr(ExprFollower::NONE);

    Limit* limit = Limit::create(_ast, count);
    locate(limit, start);

    return limit;
}

WhereClause* CypherGrammar::parseOptionalWhereClause() {
    if (check(CypherTokenKind::WHERE)) {
        return parseWhereClause(ExprFollower::NONE);
    }

    return nullptr;
}

WhereClause* CypherGrammar::parseWhereClause(ExprFollower follower) {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::WHERE);

    Expr* predicate = parseExpr(follower);

    WhereClause* where = WhereClause::create(_ast, predicate);
    locate(where, start);

    return where;
}

MatchStmt* CypherGrammar::parseMatchStmt() {
    const SourcePosition start = currentPosition();
    const bool optional = accept(CypherTokenKind::OPTIONAL);
    expect(CypherTokenKind::MATCH);

    Pattern* pattern = parsePatternWhere();
    OrderBy* orderBy = parseOptionalOrderBySubStmt();
    Skip* skip = parseOptionalSkipSubStmt();
    Limit* limit = parseOptionalLimitSubStmt();

    MatchStmt* match = MatchStmt::create(_ast, pattern);
    match->setOrderBy(orderBy);
    match->setSkip(skip);
    match->setLimit(limit);

    if (optional) {
        match->setOptional(true);
    }

    locate(match, start);

    return match;
}

ShortestPathStmt* CypherGrammar::parseShortestPathStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SHORTESTPATH);
    expect(CypherTokenKind::OPAREN);

    Symbol* source = parseSymbol();
    expect(CypherTokenKind::COMMA);
    Symbol* target = parseSymbol();
    expect(CypherTokenKind::COMMA);
    Symbol* edgeProperty = parseName();
    expect(CypherTokenKind::COMMA);
    Symbol* distance = parseSymbol();
    expect(CypherTokenKind::COMMA);
    Symbol* path = parseSymbol();
    expect(CypherTokenKind::CPAREN);

    ShortestPathStmt* stmt = ShortestPathStmt::create(_ast, source, target, edgeProperty, distance, path);
    locate(stmt, start);

    return stmt;
}

UnwindStmt* CypherGrammar::parseUnwindStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::UNWIND);

    Expr* list = parseExpr(ExprFollower::AS);
    expect(CypherTokenKind::AS);
    Symbol* name = parseName();

    UnwindStmt* stmt = UnwindStmt::create(_ast, list, name);
    locate(stmt, start);

    return stmt;
}

LoadCSVStmt* CypherGrammar::parseLoadCSVStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::LOAD);
    expect(CypherTokenKind::CSV);

    Expr* path = parseLiteralOrParameterExpr(CypherTokenKind::STRING_LITERAL);

    const bool hasHeaders = accept(CypherTokenKind::WITH);
    if (hasHeaders) {
        expect(CypherTokenKind::HEADERS);
    }

    bool skipsOnError = false;
    if (accept(CypherTokenKind::ON)) {
        expect(CypherTokenKind::ERROR_);

        skipsOnError = accept(CypherTokenKind::SKIP);
        if (!skipsOnError) {
            expect(CypherTokenKind::FAIL);
        }
    }

    expect(CypherTokenKind::AS);
    Symbol* alias = parseName();

    LoadCSVStmt* stmt = LoadCSVStmt::create(_ast, path, alias);

    if (hasHeaders) {
        stmt->setHasHeaders(true);
    }

    if (skipsOnError) {
        stmt->setSkipOnError(true);
    }

    locate(stmt, start);

    return stmt;
}

VectorSearchStmt* CypherGrammar::parseVectorSearchStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::VECTOR);
    expect(CypherTokenKind::SEARCH);
    expect(CypherTokenKind::IN);

    Symbol* index = parseName();

    expect(CypherTokenKind::FOR);
    Expr* neighbourCount = parseLiteralOrParameterExpr(CypherTokenKind::DIGIT);

    Expr* queryVector = parseLiteralOrParameterExpr(CypherTokenKind::OPAREN);
    YieldClause* yield = parseYieldClause();

    VectorSearchStmt* stmt = VectorSearchStmt::create(_ast);
    stmt->setIndexName(index->getName());
    stmt->setK(neighbourCount);
    stmt->setQueryVector(queryVector);
    stmt->setYield(yield);
    locate(stmt, start);

    return stmt;
}

CallStmt* CypherGrammar::parseCallStmt() {
    const SourcePosition start = currentPosition();
    const bool optional = accept(CypherTokenKind::OPTIONAL);
    expect(CypherTokenKind::CALL);

    Symbol* first = parseSymbol();
    QualifiedName* name = parseInvocationName(first);
    ExprChain* arguments = parseParenExprChain();

    YieldClause* yield = nullptr;
    if (check(CypherTokenKind::YIELD)) {
        yield = parseYieldClause();
    }

    CallStmt* call = CallStmt::create(_ast);
    FunctionInvocation* invocation = FunctionInvocation::create(_ast, name);
    invocation->setArguments(arguments);
    FunctionInvocationExpr* expr = FunctionInvocationExpr::create(_ast, invocation);
    call->setFunc(expr);

    if (optional) {
        call->setOptional(true);
    }

    if (yield) {
        call->setYield(yield);
    }

    locate(call, start);

    return call;
}

CallSubqueryStmt* CypherGrammar::parseCallSubqueryStmt() {
    const SourcePosition start = currentPosition();
    const bool optional = accept(CypherTokenKind::OPTIONAL);
    expect(CypherTokenKind::CALL);

    std::vector<const Symbol*> imports;
    bool importsEverything = false;

    const bool hasScopeClause = accept(CypherTokenKind::OPAREN);
    if (hasScopeClause) {
        if (accept(CypherTokenKind::MULT)) {
            importsEverything = true;
        } else if (!check(CypherTokenKind::CPAREN)) {
            parseCallScope(imports);
        }

        expect(CypherTokenKind::CPAREN);
    }

    expect(CypherTokenKind::OBRACE);
    RegularQuery* body = parseQueryBody();
    expect(CypherTokenKind::CBRACE);

    CallSubqueryStmt* call = CallSubqueryStmt::create(_ast, body);

    if (hasScopeClause) {
        call->setHasScopeClause(true);
    }

    for (const Symbol* symbol : imports) {
        call->addImport(symbol);
    }

    if (importsEverything) {
        call->setImportsEverything(true);
    }

    if (optional) {
        call->setOptional(true);
    }

    locate(call, start);

    return call;
}

void CypherGrammar::parseCallScope(std::vector<const Symbol*>& imports) {
    imports.push_back(parseSymbol());

    while (accept(CypherTokenKind::COMMA)) {
        imports.push_back(parseSymbol());
    }
}

ExprChain* CypherGrammar::parseParenExprChain() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::OPAREN);

    if (accept(CypherTokenKind::CPAREN)) {
        ExprChain* arguments = ExprChain::create(_ast);
        locate(arguments, start);

        return arguments;
    }

    ExprChain* arguments = parseExprChain();
    expect(CypherTokenKind::CPAREN);
    locate(arguments, start);

    return arguments;
}

ExprChain* CypherGrammar::parseExprChain() {
    const SourcePosition start = currentPosition();

    Expr* first = parseExpr(ExprFollower::NONE);

    ExprChain* chain = ExprChain::create(_ast);
    chain->add(first);
    locate(chain, start);

    while (accept(CypherTokenKind::COMMA)) {
        chain->add(parseExpr(ExprFollower::NONE));
        locate(chain, start);
    }

    return chain;
}

YieldClause* CypherGrammar::parseYieldClause() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::YIELD);

    if (accept(CypherTokenKind::MULT)) {
        YieldClause* yield = YieldClause::create(_ast);
        locate(yield, start);

        return yield;
    }

    YieldItems* items = parseYieldItems();

    YieldClause* yield = YieldClause::create(_ast);
    yield->setItems(items);
    locate(yield, start);

    return yield;
}

YieldItems* CypherGrammar::parseYieldItems() {
    const SourcePosition start = currentPosition();

    YieldItems* items = parseYieldItemChain();

    if (check(CypherTokenKind::WHERE)) {
        items->setWhere(parseWhereClause(ExprFollower::NONE));
    }

    locate(items, start);

    return items;
}

YieldItems* CypherGrammar::parseYieldItemChain() {
    const SourcePosition start = currentPosition();

    SymbolExpr* first = parseYieldItem();

    YieldItems* items = YieldItems::create(_ast);
    items->add(first);
    locate(items, start);

    while (accept(CypherTokenKind::COMMA)) {
        items->add(parseYieldItem());
        locate(items, start);
    }

    return items;
}

SymbolExpr* CypherGrammar::parseYieldItem() {
    const SourcePosition start = currentPosition();

    Symbol* name = parseName();

    if (accept(CypherTokenKind::AS)) {
        Symbol* alias = parseName();
        name->setAs(alias->getName());
    }

    SymbolExpr* item = SymbolExpr::create(_ast, name);
    locate(item, start);

    return item;
}

RegularQuery* CypherGrammar::parseQueryBody() {
    const SourcePosition start = currentPosition();

    RegularQuery* segment = parseNextSegment();
    if (!check(CypherTokenKind::NEXT)) {
        return segment;
    }

    return parseNextQuery(segment, start);
}

ConditionalQuery* CypherGrammar::parseConditionalBranches() {
    const SourcePosition start = currentPosition();

    ConditionalQuery::Branches branches;
    parseWhenBranches(branches);

    if (accept(CypherTokenKind::ELSE)) {
        branches.push_back({nullptr, parseConditionalBranchQuery()});
    }

    ConditionalQuery* query = ConditionalQuery::create(_ast, branches);
    locate(query, start);

    return query;
}

void CypherGrammar::parseWhenBranches(ConditionalQuery::Branches& branches) {
    do {
        expect(CypherTokenKind::WHEN);
        Expr* condition = parseExpr(ExprFollower::NONE);
        expect(CypherTokenKind::THEN);
        RegularQuery* query = parseConditionalBranchQuery();

        branches.push_back({condition, query});
    } while (check(CypherTokenKind::WHEN));
}

RegularQuery* CypherGrammar::parseConditionalBranchQuery() {
    if (accept(CypherTokenKind::OBRACE)) {
        RegularQuery* body = parseQueryBody();
        expect(CypherTokenKind::CBRACE);

        return body;
    }

    return parseSinglePartQuery();
}

CallSubqueryStmt* CypherGrammar::parseForeachStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::FOREACH);
    expect(CypherTokenKind::OPAREN);

    Symbol* variable = parseName();
    expect(CypherTokenKind::IN);

    Expr* list = nullptr;
    StmtContainer* updates = nullptr;
    parseChoosingBar([this, &list, &updates] {
        list = parseExpr(ExprFollower::PIPE);
        expect(CypherTokenKind::PIPE);
        updates = parseForeachUpdates();
        expect(CypherTokenKind::CPAREN);

        return true;
    });

    return ParserUtils::createForeach(_ast, variable, list, updates, locationFrom(start));
}

StmtContainer* CypherGrammar::parseForeachUpdates() {
    const SourcePosition start = currentPosition();

    Stmt* first = parseUpdatingStatement();

    StmtContainer* updates = StmtContainer::create(_ast);
    updates->add(first);
    locate(updates, start);

    while (startsUpdatingStatement()) {
        updates->add(parseUpdatingStatement());
    }

    return updates;
}

CreateStmt* CypherGrammar::parseCreateStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::CREATE);

    Pattern* pattern = parsePattern();

    CreateStmt* stmt = CreateStmt::create(_ast, pattern);
    locate(stmt, start);

    return stmt;
}

MergeStmt* CypherGrammar::parseMergeStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::MERGE);

    const SourcePosition partStart = currentPosition();
    PatternElement* part = parsePatternPart();
    const SourceLocation partLocation = locationFrom(partStart);

    std::pair<SetStmt*, SetStmt*> actions {nullptr, nullptr};
    const bool hasActions = check(CypherTokenKind::ON);
    if (hasActions) {
        actions = parseMergeActionChain();
    }

    Pattern* pattern = Pattern::create(_ast);
    pattern->addElement(part);

    MergeStmt* merge = MergeStmt::create(_ast, pattern);

    if (hasActions) {
        merge->setOnCreate(actions.first);
        merge->setOnMatch(actions.second);
    }

    _sourceManager->setLocation(pattern, partLocation);
    locate(merge, start);

    return merge;
}

std::pair<SetStmt*, SetStmt*> CypherGrammar::parseMergeActionChain() {
    std::pair<SetStmt*, SetStmt*> chain = parseMergeAction();

    while (check(CypherTokenKind::ON)) {
        const std::pair<SetStmt*, SetStmt*> action = parseMergeAction();
        ParserUtils::mergeSetClauses(chain.first, action.first);
        ParserUtils::mergeSetClauses(chain.second, action.second);
    }

    return chain;
}

std::pair<SetStmt*, SetStmt*> CypherGrammar::parseMergeAction() {
    expect(CypherTokenKind::ON);

    if (accept(CypherTokenKind::MATCH)) {
        return {nullptr, parseSetStmt()};
    }

    expect(CypherTokenKind::CREATE);

    return {parseSetStmt(), nullptr};
}

SetStmt* CypherGrammar::parseSetStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::SET);

    SetItem* first = parseSetItem();

    SetStmt* stmt = SetStmt::create(_ast);
    stmt->addItem(first);
    locate(stmt, start);

    while (accept(CypherTokenKind::COMMA)) {
        stmt->addItem(parseSetItem());
    }

    return stmt;
}

SetItem* CypherGrammar::parseSetItem() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::OPAREN)) {
        PropertyExpr* property = parseVariablePropertyExpr();
        expect(CypherTokenKind::ASSIGN);
        Expr* value = parseExpr(ExprFollower::NONE);

        SetItem* item = SetItem::create(_ast, property, value);
        locate(item, start);

        return item;
    }

    Symbol* symbol = parseSymbol();

    SetItem* item = nullptr;
    if (check(CypherTokenKind::DOT)) {
        QualifiedName* name = parseQualifiedName(symbol);
        PropertyExpr* property = parsePropertyExpr(name, start);
        expect(CypherTokenKind::ASSIGN);
        Expr* value = parseExpr(ExprFollower::NONE);

        item = SetItem::create(_ast, property, value);
    } else if (check(CypherTokenKind::ASSIGN) || check(CypherTokenKind::ADD_ASSIGN)) {
        const SourceLocation symbolLocation = locationFrom(start);
        const bool replaces = check(CypherTokenKind::ASSIGN);
        consume();

        Expr* value = parseExpr(ExprFollower::NONE);

        SymbolExpr* entity = SymbolExpr::create(_ast, symbol);
        _sourceManager->setLocation(entity, symbolLocation);

        item = SetItem::create(_ast, entity, value, replaces);
    } else if (check(CypherTokenKind::COLON) || check(CypherTokenKind::IS)) {
        EntityTypeExpr* entity = parseEntityTypeExpr(symbol, start);

        item = SetItem::create(_ast, entity);
    } else {
        throwUnexpectedToken();
    }

    locate(item, start);

    return item;
}

RemoveStmt* CypherGrammar::parseRemoveStmt() {
    const SourcePosition start = currentPosition();
    expect(CypherTokenKind::REMOVE);

    PropertyExpr* first = parseRemoveItem();

    RemoveStmt* stmt = RemoveStmt::create(_ast);
    stmt->addProperty(first);
    locate(stmt, start);

    while (accept(CypherTokenKind::COMMA)) {
        stmt->addProperty(parseRemoveItem());
    }

    return stmt;
}

PropertyExpr* CypherGrammar::parseRemoveItem() {
    const SourcePosition start = currentPosition();

    if (check(CypherTokenKind::OPAREN)) {
        return parseVariablePropertyExpr();
    }

    Symbol* symbol = parseSymbol();

    if (check(CypherTokenKind::DOT)) {
        QualifiedName* name = parseQualifiedName(symbol);
        return parsePropertyExpr(name, start);
    } else if (check(CypherTokenKind::COLON) || check(CypherTokenKind::IS)) {
        parseEntityTypeExpr(symbol, start);
        reportNotImplemented(locationFrom(start), "REMOVE of a label");

        return nullptr;
    }

    throwUnexpectedToken();
}

DeleteStmt* CypherGrammar::parseDeleteStmt() {
    const SourcePosition start = currentPosition();
    const bool detaching = accept(CypherTokenKind::DETACH);
    expect(CypherTokenKind::DELETE);

    ExprChain* targets = parseExprChain();

    DeleteStmt* stmt = DeleteStmt::create(_ast, targets);

    if (detaching) {
        stmt->setDetaching(true);
    }

    locate(stmt, start);

    return stmt;
}

bool CypherGrammar::startsQueryStatement() {
    return startsReadingStatement() || startsUpdatingStatement();
}

bool CypherGrammar::startsReadingStatement() {
    return check(CypherTokenKind::MATCH)
           || check(CypherTokenKind::OPTIONAL)
           || check(CypherTokenKind::WITH)
           || check(CypherTokenKind::UNWIND)
           || check(CypherTokenKind::CALL)
           || check(CypherTokenKind::LOAD)
           || check(CypherTokenKind::VECTOR);
}

bool CypherGrammar::startsUpdatingStatement() {
    return check(CypherTokenKind::CREATE)
           || check(CypherTokenKind::MERGE)
           || check(CypherTokenKind::DELETE)
           || check(CypherTokenKind::DETACH)
           || check(CypherTokenKind::SET)
           || check(CypherTokenKind::REMOVE)
           || check(CypherTokenKind::FOREACH);
}

bool CypherGrammar::opensCallSubquery(size_t offset) {
    const CypherTokenKind kind = peek(offset)._kind;
    return kind == CypherTokenKind::OPAREN || kind == CypherTokenKind::OBRACE;
}

bool CypherGrammar::readsBracedPattern() {
    const CypherTokenKind first = examineKind();

    if (first == CypherTokenKind::OPAREN) {
        return true;
    } else if (first == CypherTokenKind::OBRACE) {
        return false;
    } else if (!CypherToken::isName(first)) {
        throwUnexpectedTokenAt(0, getNameKinds() | kindsOf({CypherTokenKind::OPAREN, CypherTokenKind::OBRACE}));
    } else if (peek(1)._kind == CypherTokenKind::ASSIGN || !opensClause(first)) {
        return true;
    }

    addPending(CypherTokenKind::ASSIGN);

    return false;
}
