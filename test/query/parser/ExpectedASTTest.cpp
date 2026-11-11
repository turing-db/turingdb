#include "ExpectedASTTest.h"

#include <algorithm>
#include <string>

#include "CypherAST.h"
#include "CypherASTComparator.h"
#include "CypherParser.h"
#include "FunctionInvocation.h"
#include "LabelExpr.h"
#include "Parameter.h"
#include "ParseOutcome.h"
#include "ParserException.h"
#include "QualifiedName.h"
#include "QueryMutants.h"
#include "Symbol.h"
#include "SymbolChain.h"
#include "YieldItems.h"
#include "stmt/StmtContainer.h"

using namespace db;
using namespace turing::test;

ExpectedASTTest::ExpectedASTTest()
{
    resetExpectedAST();
}

ExpectedASTTest::~ExpectedASTTest() {
}

void ExpectedASTTest::expectParse(std::string_view query, std::initializer_list<const QueryCommand*> queries) {
    const bool listsTheExpectedQueries = std::ranges::equal(_expected->queries(), queries);
    EXPECT_TRUE(listsTheExpectedQueries) << query << "\nThe expected AST holds other top-level queries than the ones listed";

    CypherAST actual(nullptr, query);
    CypherParser parser(&actual);

    try {
        parser.parse(query);

        std::string difference;
        EXPECT_TRUE(CypherASTComparator::compare(*_expected, actual, difference)) << query << "\n" << difference;
    } catch (const ParserException& error) {
        ADD_FAILURE() << query << "\n" << error.what();
    }

    expectMutantsWellFormed(query);
    resetExpectedAST();
}

void ExpectedASTTest::expectError(std::string_view query, std::string_view report) {
    std::string actualReport;
    renderParseError(query, actualReport);
    EXPECT_EQ(actualReport, report) << query;

    expectMutantsWellFormed(query);
    resetExpectedAST();
}

Symbol* ExpectedASTTest::name(std::string_view spelling) {
    return Symbol::create(_expected.get(), spelling);
}

LiteralExpr* ExpectedASTTest::literal(Literal* value) {
    return LiteralExpr::create(_expected.get(), value);
}

LiteralExpr* ExpectedASTTest::integer(int64_t value) {
    return literal(IntegerLiteral::create(_expected.get(), value));
}

LiteralExpr* ExpectedASTTest::real(double value) {
    return literal(DoubleLiteral::create(_expected.get(), value));
}

LiteralExpr* ExpectedASTTest::string(std::string_view value) {
    return literal(StringLiteral::create(_expected.get(), value));
}

LiteralExpr* ExpectedASTTest::boolean(bool value) {
    return literal(BoolLiteral::create(_expected.get(), value));
}

LiteralExpr* ExpectedASTTest::null() {
    return literal(NullLiteral::create(_expected.get()));
}

LiteralExpr* ExpectedASTTest::wildcard() {
    return literal(WildcardLiteral::create(_expected.get()));
}

LiteralExpr* ExpectedASTTest::normalForm(UnicodeNormalForm form) {
    return literal(NormalFormLiteral::create(_expected.get(), form));
}

LiteralExpr* ExpectedASTTest::list(std::initializer_list<Expr*> items) {
    ListLiteral* values = ListLiteral::create(_expected.get());
    for (Expr* item : items) {
        values->addItem(item);
    }

    return literal(values);
}

LiteralExpr* ExpectedASTTest::map(std::initializer_list<std::pair<std::string_view, Expr*>> entries) {
    return literal(properties(entries));
}

MapLiteral* ExpectedASTTest::properties(std::initializer_list<std::pair<std::string_view, Expr*>> entries) {
    MapLiteral* values = MapLiteral::create(_expected.get());
    for (const auto& [key, value] : entries) {
        values->addEntry(name(key), value);
    }

    return values;
}

EmbeddingLiteral* ExpectedASTTest::embedding(std::initializer_list<float> values) {
    EmbeddingLiteral* vector = EmbeddingLiteral::create(_expected.get());
    for (const float value : values) {
        vector->addItem(static_cast<double>(value));
    }

    return vector;
}

SymbolExpr* ExpectedASTTest::symbol(std::string_view spelling) {
    return SymbolExpr::create(_expected.get(), name(spelling));
}

PropertyExpr* ExpectedASTTest::property(std::initializer_list<std::string_view> names) {
    QualifiedName* fullName = QualifiedName::create(_expected.get());
    for (const std::string_view part : names) {
        fullName->addName(name(part));
    }

    return PropertyExpr::create(_expected.get(), fullName);
}

PropertyLookupExpr* ExpectedASTTest::lookup(Expr* base, std::string_view propertyName) {
    return PropertyLookupExpr::create(_expected.get(), base, propertyName);
}

ParameterExpr* ExpectedASTTest::parameter(std::string_view spelling) {
    return ParameterExpr::create(_expected.get(), Parameter::create(_expected.get(), spelling));
}

BinaryExpr* ExpectedASTTest::binary(BinaryOperator binaryOperator, Expr* lhs, Expr* rhs) {
    return BinaryExpr::create(_expected.get(), binaryOperator, lhs, rhs);
}

UnaryExpr* ExpectedASTTest::unary(UnaryOperator unaryOperator, Expr* operand) {
    return UnaryExpr::create(_expected.get(), unaryOperator, operand);
}

StringExpr* ExpectedASTTest::stringExpr(StringOperator stringOperator, Expr* lhs, Expr* rhs) {
    return StringExpr::create(_expected.get(), stringOperator, lhs, rhs);
}

LogicalExpr* ExpectedASTTest::logical(LogicalOperator logicalOperator, std::initializer_list<Expr*> operands) {
    LogicalExpr* expr = LogicalExpr::create(_expected.get(), logicalOperator);
    for (Expr* operand : operands) {
        expr->addOperand(operand);
    }

    return expr;
}

LogicalExpr* ExpectedASTTest::comparisonChain(Expr* first, std::initializer_list<std::pair<BinaryOperator, Expr*>> links) {
    Expr* lhs = first;
    Expr* firstLink = nullptr;
    LogicalExpr* chain = nullptr;

    for (const auto& [comparison, rhs] : links) {
        BinaryExpr* link = binary(comparison, lhs, rhs);

        if (!firstLink) {
            firstLink = link;
        } else {
            chain = LogicalExpr::appendComparison(_expected.get(), chain ? chain : firstLink, link);
        }

        lhs = rhs;
    }

    EXPECT_NE(chain, nullptr) << "A comparison chain links at least two comparisons";

    return chain;
}

IndexExpr* ExpectedASTTest::index(Expr* base, Expr* position) {
    return IndexExpr::create(_expected.get(), base, position);
}

ListSliceExpr* ExpectedASTTest::slice(Expr* base, Expr* from, Expr* to) {
    return ListSliceExpr::create(_expected.get(), base, from, to);
}

ListComprehensionExpr* ExpectedASTTest::listComprehension(std::string_view variable,
                                                          Expr* source,
                                                          Expr* predicate,
                                                          Expr* projection) {
    ListComprehensionExpr* comprehension = ListComprehensionExpr::create(_expected.get(), name(variable), source);
    comprehension->setPredicate(predicate);
    comprehension->setProjection(projection);

    return comprehension;
}

ListPredicateExpr* ExpectedASTTest::listPredicate(ListPredicateExpr::Quantifier quantifier,
                                                  ListComprehensionExpr* comprehension) {
    return ListPredicateExpr::create(_expected.get(), quantifier, comprehension);
}

PatternComprehensionExpr* ExpectedASTTest::patternComprehension(MatchStmt* match, Expr* projection) {
    return PatternComprehensionExpr::create(_expected.get(), match, projection);
}

ReduceExpr* ExpectedASTTest::reduce(std::string_view accumulator,
                                    Expr* initialValue,
                                    std::string_view item,
                                    Expr* source,
                                    Expr* expression) {
    return ReduceExpr::create(_expected.get(), name(accumulator), initialValue, name(item), source, expression);
}

AllReduceExpr* ExpectedASTTest::allReduce(std::string_view accumulator,
                                          Expr* initialValue,
                                          std::string_view item,
                                          Expr* source,
                                          Expr* expression,
                                          Expr* predicate) {
    return AllReduceExpr::create(_expected.get(),
                                 name(accumulator),
                                 initialValue,
                                 name(item),
                                 source,
                                 expression,
                                 predicate);
}

CaseExpr* ExpectedASTTest::caseExpr(Expr* subject, std::initializer_list<CaseExpr::Branch> branches, Expr* elseExpr) {
    CaseExpr* expr = CaseExpr::create(_expected.get());
    expr->setSubject(subject);

    for (const CaseExpr::Branch& branch : branches) {
        expr->addBranch(branch._tests, branch._then);
    }

    expr->setElseExpr(elseExpr);

    return expr;
}

FunctionInvocationExpr* ExpectedASTTest::invocation(std::initializer_list<std::string_view> names,
                                                    std::initializer_list<Expr*> arguments) {
    QualifiedName* functionName = QualifiedName::create(_expected.get());
    for (const std::string_view part : names) {
        functionName->addName(name(part));
    }

    ExprChain* argumentChain = ExprChain::create(_expected.get());
    for (Expr* argument : arguments) {
        argumentChain->add(argument);
    }

    FunctionInvocation* function = FunctionInvocation::create(_expected.get(), functionName);
    function->setArguments(argumentChain);

    return FunctionInvocationExpr::create(_expected.get(), function);
}

FunctionInvocationExpr* ExpectedASTTest::distinct(FunctionInvocationExpr* invocation) {
    FunctionInvocation* function = invocation->getFunctionInvocation();
    function->setDistinct(true);

    return invocation;
}

ExistsExpr* ExpectedASTTest::exists(RegularQuery* body) {
    return ExistsExpr::create(_expected.get(), body);
}

ExistsExpr* ExpectedASTTest::patternPredicate(Pattern* predicate) {
    SinglePartQuery* body = query({match({._pattern = predicate})}, nullptr);

    ExistsExpr* expr = exists(body);
    expr->setPredicatePattern(predicate);

    return expr;
}

CountSubqueryExpr* ExpectedASTTest::countSubquery(RegularQuery* body) {
    return CountSubqueryExpr::create(_expected.get(), body);
}

CollectSubqueryExpr* ExpectedASTTest::collectSubquery(RegularQuery* body) {
    return CollectSubqueryExpr::create(_expected.get(), body);
}

MapProjectionExpr* ExpectedASTTest::mapProjection(std::string_view base,
                                                  std::initializer_list<MapProjectionExpr::Item> items) {
    return MapProjectionExpr::create(_expected.get(), symbol(base), MapProjectionExpr::Items(items));
}

EntityTypeExpr* ExpectedASTTest::entityType(std::string_view variable, LabelExpr* labels) {
    return EntityTypeExpr::create(_expected.get(), name(variable), labels);
}

EntityTypeExpr* ExpectedASTTest::labelTest(Expr* operand, LabelExpr* labels) {
    return EntityTypeExpr::create(_expected.get(), operand, labels);
}

NodePattern* ExpectedASTTest::node(const NodeFields& fields) {
    NodePattern* entity = NodePattern::create(_expected.get());
    entity->setSymbol(fields._symbol.empty() ? nullptr : name(fields._symbol));
    entity->setLabelExpr(fields._labels);
    entity->setProperties(fields._properties);
    entity->setWhere(fields._where);

    return entity;
}

EdgePattern* ExpectedASTTest::edge(const EdgeFields& fields) {
    EdgePattern* entity = EdgePattern::create(_expected.get(), fields._range, fields._direction);
    entity->setSymbol(fields._symbol.empty() ? nullptr : name(fields._symbol));
    entity->setLabelExpr(fields._labels);
    entity->setProperties(fields._properties);
    entity->setWhere(fields._where);

    return entity;
}

EdgePattern* ExpectedASTTest::walk(const WalkFields& fields) {
    EdgePattern* entity = EdgePattern::create(_expected.get(), fields._quantifier, fields._direction);

    for (EdgePattern* hop : fields._hops) {
        entity->addHop(hop);
    }

    for (NodePattern* hopNode : fields._hopNodes) {
        entity->addHopNode(hopNode);
    }

    entity->setHopWhere(fields._hopWhere);

    return entity;
}

QuantifiedPath* ExpectedASTTest::quantifier(int64_t lower, int64_t upper) {
    QuantifiedPath* bounds = QuantifiedPath::create(_expected.get());
    bounds->setLhs(lower);
    bounds->setRhs(upper);

    return bounds;
}

LabelExpr* ExpectedASTTest::labels(std::initializer_list<std::string_view> names) {
    SymbolChain* chain = SymbolChain::create(_expected.get());
    for (const std::string_view label : names) {
        chain->add(name(label));
    }

    return LabelExpr::createLabels(_expected.get(), chain);
}

LabelExpr* ExpectedASTTest::labelParameter(std::string_view spelling) {
    SymbolChain* chain = SymbolChain::create(_expected.get());
    chain->addParameter(Parameter::create(_expected.get(), spelling));

    return LabelExpr::createLabels(_expected.get(), chain);
}

LabelExpr* ExpectedASTTest::labelNot(LabelExpr* operand) {
    return LabelExpr::createNot(_expected.get(), operand);
}

LabelExpr* ExpectedASTTest::labelAnd(std::initializer_list<LabelExpr*> operands) {
    LabelExpr* conjunction = nullptr;
    for (LabelExpr* operand : operands) {
        conjunction = conjunction ? LabelExpr::createAnd(_expected.get(), conjunction, operand) : operand;
    }

    return conjunction;
}

LabelExpr* ExpectedASTTest::labelOr(std::initializer_list<LabelExpr*> operands) {
    LabelExpr* disjunction = nullptr;
    for (LabelExpr* operand : operands) {
        disjunction = disjunction ? LabelExpr::createOr(_expected.get(), disjunction, operand) : operand;
    }

    return disjunction;
}

PatternElement* ExpectedASTTest::path(std::initializer_list<EntityPattern*> entities) {
    PatternElement* element = PatternElement::create(_expected.get());
    for (EntityPattern* entity : entities) {
        element->addEntity(entity);
    }

    return element;
}

PatternElement* ExpectedASTTest::namedPath(std::string_view pathName, std::initializer_list<EntityPattern*> entities) {
    PatternElement* element = path(entities);
    element->setPathSymbol(name(pathName));

    return element;
}

Pattern* ExpectedASTTest::pattern(std::initializer_list<PatternElement*> elements, WhereClause* filter) {
    Pattern* matched = Pattern::create(_expected.get());
    for (PatternElement* element : elements) {
        matched->addElement(element);
    }

    matched->setWhere(filter);

    return matched;
}

WhereClause* ExpectedASTTest::where(Expr* predicate) {
    return WhereClause::create(_expected.get(), predicate);
}

MatchStmt* ExpectedASTTest::match(const MatchFields& fields) {
    MatchStmt* stmt = MatchStmt::create(_expected.get(), fields._pattern);
    stmt->setOptional(fields._optional);
    stmt->setOrderBy(fields._orderBy);
    stmt->setSkip(fields._skip);
    stmt->setLimit(fields._limit);

    return stmt;
}

Projection* ExpectedASTTest::projection(const ProjectionFields& fields) {
    Projection* columns = Projection::create(_expected.get());
    for (Expr* item : fields._items) {
        columns->addExpr(item);
    }

    columns->setDistinct(fields._distinct);

    if (fields._all) {
        columns->setReturnAll();
    }

    columns->setOrderBy(fields._orderBy);
    columns->setSkip(fields._skip);
    columns->setLimit(fields._limit);

    return columns;
}

ReturnStmt* ExpectedASTTest::returning(std::initializer_list<Expr*> items) {
    return returning(projection({._items = items}));
}

ReturnStmt* ExpectedASTTest::returning(Projection* columns) {
    return ReturnStmt::create(_expected.get(), columns);
}

WithStmt* ExpectedASTTest::with(const WithFields& fields) {
    WithStmt* stmt = WithStmt::create(_expected.get(), fields._projection);
    stmt->setWhere(fields._where);
    stmt->setSegmentReturn(fields._segmentReturn);

    return stmt;
}

UnwindStmt* ExpectedASTTest::unwind(Expr* list, std::string_view variable) {
    return UnwindStmt::create(_expected.get(), list, name(variable));
}

CallStmt* ExpectedASTTest::call(const CallFields& fields) {
    CallStmt* stmt = CallStmt::create(_expected.get());
    stmt->setFunc(fields._invocation);
    stmt->setYield(fields._yield);
    stmt->setOptional(fields._optional);
    stmt->setStandaloneCall(fields._standalone);

    return stmt;
}

CallSubqueryStmt* ExpectedASTTest::callSubquery(const SubqueryFields& fields) {
    CallSubqueryStmt* stmt = CallSubqueryStmt::create(_expected.get(), fields._body);
    for (const std::string_view imported : fields._imports) {
        stmt->addImport(name(imported));
    }

    stmt->setHasScopeClause(fields._scope);
    stmt->setImportsEverything(fields._importsEverything);
    stmt->setOptional(fields._optional);

    return stmt;
}

NextSegmentStmt* ExpectedASTTest::nextSegment(CallSubqueryStmt* body) {
    return NextSegmentStmt::create(_expected.get(), body);
}

CreateStmt* ExpectedASTTest::createStmt(Pattern* created) {
    return CreateStmt::create(_expected.get(), created);
}

MergeStmt* ExpectedASTTest::merge(Pattern* merged, SetStmt* onCreate, SetStmt* onMatch) {
    MergeStmt* stmt = MergeStmt::create(_expected.get(), merged);
    stmt->setOnCreate(onCreate);
    stmt->setOnMatch(onMatch);

    return stmt;
}

SetStmt* ExpectedASTTest::set(std::initializer_list<SetItem*> items) {
    SetStmt* stmt = SetStmt::create(_expected.get());
    for (SetItem* item : items) {
        stmt->addItem(item);
    }

    return stmt;
}

SetItem* ExpectedASTTest::setProperty(PropertyExpr* target, Expr* value) {
    return SetItem::create(_expected.get(), target, value);
}

SetItem* ExpectedASTTest::setLabels(EntityTypeExpr* labelsSet) {
    return SetItem::create(_expected.get(), labelsSet);
}

SetItem* ExpectedASTTest::addProperties(std::string_view variable, Expr* value) {
    return SetItem::create(_expected.get(), symbol(variable), value, false);
}

SetItem* ExpectedASTTest::replaceProperties(std::string_view variable, Expr* value) {
    return SetItem::create(_expected.get(), symbol(variable), value, true);
}

RemoveStmt* ExpectedASTTest::remove(std::initializer_list<PropertyExpr*> removed) {
    RemoveStmt* stmt = RemoveStmt::create(_expected.get());
    for (PropertyExpr* property : removed) {
        stmt->addProperty(property);
    }

    return stmt;
}

DeleteStmt* ExpectedASTTest::deleteStmt(std::initializer_list<Expr*> deleted) {
    ExprChain* chain = ExprChain::create(_expected.get());
    for (Expr* expr : deleted) {
        chain->add(expr);
    }

    return DeleteStmt::create(_expected.get(), chain);
}

DeleteStmt* ExpectedASTTest::detachDelete(std::initializer_list<Expr*> deleted) {
    DeleteStmt* stmt = deleteStmt(deleted);
    stmt->setDetaching(true);

    return stmt;
}

OrderBy* ExpectedASTTest::orderBy(std::initializer_list<std::pair<Expr*, OrderByType>> items) {
    OrderBy* order = OrderBy::create(_expected.get());
    for (const auto& [key, direction] : items) {
        order->addItem(OrderByItem::create(_expected.get(), key, direction));
    }

    return order;
}

Skip* ExpectedASTTest::skip(Expr* count) {
    return Skip::create(_expected.get(), count);
}

Limit* ExpectedASTTest::limit(Expr* count) {
    return Limit::create(_expected.get(), count);
}

YieldClause* ExpectedASTTest::yield(std::initializer_list<SymbolExpr*> items, WhereClause* filter) {
    YieldItems* yielded = YieldItems::create(_expected.get());
    for (SymbolExpr* item : items) {
        yielded->add(item);
    }

    yielded->setWhere(filter);

    YieldClause* clause = YieldClause::create(_expected.get());
    clause->setItems(yielded);

    return clause;
}

YieldClause* ExpectedASTTest::yieldAll() {
    return YieldClause::create(_expected.get());
}

SymbolExpr* ExpectedASTTest::yieldItem(std::string_view yielded, std::string_view alias) {
    Symbol* item = name(yielded);
    item->setAs(alias);

    return SymbolExpr::create(_expected.get(), item);
}

LoadCSVStmt* ExpectedASTTest::loadCSV(const LoadCSVFields& fields) {
    LoadCSVStmt* stmt = LoadCSVStmt::create(_expected.get(), fields._path, name(fields._alias));
    stmt->setHasHeaders(fields._headers);
    stmt->setSkipOnError(fields._skipOnError);

    return stmt;
}

VectorSearchStmt* ExpectedASTTest::vectorSearch(const VectorSearchFields& fields) {
    VectorSearchStmt* stmt = VectorSearchStmt::create(_expected.get());
    stmt->setIndex(fields._index);
    stmt->setK(fields._neighbours);
    stmt->setQueryVector(fields._queryVector);
    stmt->setYield(fields._yield);

    return stmt;
}

ShortestPathStmt* ExpectedASTTest::shortestPath(std::string_view source,
                                                std::string_view target,
                                                std::string_view edgeProperty,
                                                std::string_view distance,
                                                std::string_view shortest) {
    return ShortestPathStmt::create(_expected.get(),
                                    name(source),
                                    name(target),
                                    name(edgeProperty),
                                    name(distance),
                                    name(shortest));
}

SinglePartQuery* ExpectedASTTest::query(std::initializer_list<Stmt*> stmts, ReturnStmt* returnStmt) {
    SinglePartQuery* single = SinglePartQuery::create(_expected.get());

    if (stmts.size() > 0) {
        StmtContainer* container = StmtContainer::create(_expected.get());
        for (Stmt* stmt : stmts) {
            container->add(stmt);
        }

        single->setStmts(container);
    }

    single->setReturnStmt(returnStmt);

    return single;
}

UnionQuery* ExpectedASTTest::unionQuery(RegularQuery* first, std::initializer_list<UnionQuery::Branch> rest) {
    return UnionQuery::create(_expected.get(), first, UnionQuery::Branches(rest));
}

ConditionalQuery* ExpectedASTTest::conditional(std::initializer_list<ConditionalQuery::Branch> branches) {
    return ConditionalQuery::create(_expected.get(), ConditionalQuery::Branches(branches));
}

void ExpectedASTTest::expectMutantsWellFormed(std::string_view query) {
    std::vector<std::string> mutants;
    buildQueryMutants(std::string(query), mutants);

    for (const std::string& mutant : mutants) {
        std::string problem;
        EXPECT_TRUE(isWellFormedParse(mutant, problem)) << mutant << "\n" << problem;
    }
}

void ExpectedASTTest::resetExpectedAST() {
    _expected = std::make_unique<CypherAST>(nullptr, std::string_view {});
}
