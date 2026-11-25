#include "ExprAnalyzer.h"

#include <stdint.h>

#include <vector>

#include "ReadStmtAnalyzer.h"
#include "CypherAST.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "ParserUtils.h"
#include "QualifiedName.h"
#include "SourceManager.h"
#include "Symbol.h"
#include "Literal.h"
#include "stmt/LoadCSVStmt.h"
#include "decl/DeclContext.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

#include "expr/All.h"
#include "expr/ExprChildren.h"
#include "expr/Expr.h"

using namespace db;

namespace {

// The type a reduce's accumulator takes once it has held @param accumulator and then
// @param value: their common type, an integer beside a double widening as the arithmetic
// updating it does, and a tagged cell when the two share no column type
EvaluatedType accumulatedType(EvaluatedType accumulator, EvaluatedType value) {
    if (accumulator == EvaluatedType::Any) {
        return accumulator;
    }

    const EvaluatedType common = commonType(accumulator, value);
    if (common == EvaluatedType::Invalid) {
        return EvaluatedType::Any;
    }

    return common;
}

// The shape of the list these elements make, joined from the one-element list each makes:
// [1, null, 3] is a list of integers with a null in it, [1, 'a'] a list of tagged scalars,
// and [] is a list of no type, as [null, null] is.
ListShape sharedListShape(std::span<Expr* const> elements) {
    ListShape shared(EvaluatedType::Null, 1);

    for (const Expr* element : elements) {
        shared = ListShape::joined(shared, ListShape::singleton(element->getType(), element->getListShape()));
    }

    return shared;
}

}

void ExprAnalyzer::analyzeIndexExpr(IndexExpr* expr) {
    Expr* base = expr->getBase();
    Expr* indexExpr = expr->getIndexExpr();

    analyzeExpr(base);
    analyzeExpr(indexExpr);

    if (base->isDynamic() || indexExpr->isDynamic()) {
        expr->setDynamic();
    }

    if (base->isAggregate() || indexExpr->isAggregate()) {
        expr->setAggregate();
    }

    const EvaluatedType baseType = base->getType();
    const bool indexesAList = baseType == EvaluatedType::List;
    const bool indexesAMap = baseType == EvaluatedType::Map;
    const bool indexesAnEntity = baseType == EvaluatedType::NodePattern
                              || baseType == EvaluatedType::EdgePattern;
    const bool indexesACSVRow = baseType == EvaluatedType::StringTable;

    const bool indexesATaggedCell = baseType == EvaluatedType::Any;

    const bool indexesANullValue = baseType == EvaluatedType::Null;

    // A tagged cell keeps the bracket's own read: the dotted form reads the components of
    // a datetime the cell holds, which the bracket raises on
    const bool indexesByAKeyInTheQuery = indexExpr->getKind() == Expr::Kind::LITERAL
                                      && indexExpr->getType() == EvaluatedType::String;
    const bool readsAsTheDottedForm = indexesByAKeyInTheQuery
                                   && (indexesAMap || indexesAnEntity || indexesACSVRow);

    Expr* const dottedForm = readsAsTheDottedForm ? createDottedForm(expr) : nullptr;
    expr->setDottedForm(dottedForm);

    if (dottedForm) {
        analyzeExpr(dottedForm);

        if (dottedForm->isDynamic()) {
            expr->setDynamic();
        }

        expr->setExprType(dottedForm->getExprType());
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, dottedForm->getType()));

        return;
    }

    if (!indexesAList && !indexesAMap && !indexesATaggedCell && !indexesACSVRow
        && !indexesANullValue) {
        throwError(fmt::format("Index operator [] can only be applied to a list, a map or a CSV row, not '{}'",
                               EvaluatedTypeName::value(baseType)), expr);
    }

    if (indexesACSVRow) {
        analyzeCSVRowIndex(expr);
        return;
    }

    const EvaluatedType indexType = indexExpr->getType();

    const bool indexesByKey = indexType == EvaluatedType::String;
    const bool indexesByPosition = indexType == EvaluatedType::Integer;
    const bool indexesByNull = indexType == EvaluatedType::Null;
    const bool indexesByATaggedCell = indexType == EvaluatedType::Any;

    // What an index can be at all, which no base changes. Which of the two a base takes is
    // its own question, below.
    if (!indexesByKey && !indexesByPosition && !indexesByNull && !indexesByATaggedCell) {
        throwError(fmt::format("An index is an integer position or a string key, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    // Set a null index, and a read of a null value, to evaluate to a null type
    if (indexesANullValue || indexesByNull) {
        expr->setType(EvaluatedType::Null);
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Null));

        return;
    }

    // A tagged cell takes either kind, since only its row says which it holds
    if (indexesAMap && !indexesByKey && !indexesByATaggedCell) {
        throwError(fmt::format("A map is indexed by a string key, not by '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    } else if (indexesAList && !indexesByPosition && !indexesByATaggedCell) {
        throwError(fmt::format("A list index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    if (indexesByATaggedCell) {
        expr->setReadKind(IndexExpr::ReadKind::ContainerElement);
    } else if (indexesByKey) {
        expr->setReadKind(IndexExpr::ReadKind::MapKey);
    } else {
        expr->setReadKind(IndexExpr::ReadKind::ListElement);
    }

    const bool readsAListElement = indexesAList || indexesByPosition;
    if (!readsAListElement) {
        expr->setType(EvaluatedType::Any);
        expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Any));

        return;
    }

    // The case where we are reading from a list or path
    const EvaluatedType elementType = base->getListShape().unwoundType();
    const bool readsAValue = convertibleToValueType(elementType);
    const bool readsAnEntity = elementType == EvaluatedType::NodePattern
                            || elementType == EvaluatedType::EdgePattern;
    const bool readsASchemaID = namesASchemaID(elementType);
    const bool readsTheElementType = readsAValue || readsAnEntity || readsASchemaID;
    const EvaluatedType indexedType = readsTheElementType ? elementType : EvaluatedType::Any;

    expr->setType(indexedType);
    expr->setListShape(base->getListShape().unwound());

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, indexedType));
}

void ExprAnalyzer::analyzeCSVRowIndex(IndexExpr* expr) {
    const Expr* base = expr->getBase();
    const Expr* indexExpr = expr->getIndexExpr();

    const EvaluatedType indexType = indexExpr->getType();

    // A row has no null position to read, so a null index names no field here, unlike the
    // one a list or a map answers
    if (indexType != EvaluatedType::Integer) {
        throwError(fmt::format("Index expression must be an integer, not '{}'",
                               EvaluatedTypeName::value(indexType)), expr);
    }

    // Detect literal index for compile-time optimization
    if (indexExpr->getKind() == Expr::Kind::LITERAL) {
        const LiteralExpr* lit = static_cast<const LiteralExpr*>(indexExpr);
        if (lit->getLiteral()->getKind() == Literal::Kind::INTEGER) {
            const int64_t val = static_cast<const IntegerLiteral*>(lit->getLiteral())->getValue();
            if (val >= 0) {
                expr->setLiteralIndex(static_cast<size_t>(val));
            } else {
                throwError("CSV row index must be non-negative", expr);
            }
        }
    }

    expr->setType(EvaluatedType::String);
    expr->setDynamic();

    // A computed index names no field of the load: which column it reads is only known
    // once the row is in hand, so it keeps an access of its own and the engine that
    // cannot build one reports the gap
    LoadCSVStmt* const loadCSV = findCSVSource(base->getExprVarDecl());
    if (loadCSV && expr->hasLiteralIndex()) {
        const size_t slot = loadCSV->declareField(expr->getLiteralIndex());
        expr->setCSVFieldDecl(declareCSVField(*loadCSV, slot));
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::String));
}

Expr* ExprAnalyzer::createDottedForm(const IndexExpr* expr) {
    const LiteralExpr* key = static_cast<const LiteralExpr*>(expr->getIndexExpr());
    const StringLiteral* keyLiteral = static_cast<const StringLiteral*>(key->getLiteral());

    Symbol* keySymbol = Symbol::create(_ast, keyLiteral->getValue());
    Expr* dottedForm = ParserUtils::createPropertyLookup(_ast, expr->getBase(), keySymbol);

    SourceManager* sourceManager = _ast->getSourceManager();
    const SourceLocation* location = sourceManager->getLocation(reinterpret_cast<uintptr_t>(expr));
    if (location) {
        sourceManager->setLocation(dottedForm, *location);
    }

    return dottedForm;
}

void ExprAnalyzer::analyzeListSliceExpr(ListSliceExpr* expr) {
    Expr* const base = expr->getBase();
    analyzeExpr(base);

    const EvaluatedType baseType = base->getType();

    // A tagged cell names no type until a row is in hand, and a null slices into a null
    const bool slicesAList = baseType == EvaluatedType::List
                          || baseType == EvaluatedType::Any
                          || baseType == EvaluatedType::Null;

    if (!slicesAList) {
        throwError(fmt::format("A slice reads a list, not '{}'", EvaluatedTypeName::value(baseType)),
                   expr);
    }

    bool dynamic = base->isDynamic();
    bool aggregate = base->isAggregate();

    for (Expr* const bound : {expr->getFrom(), expr->getTo()}) {
        if (!bound) {
            continue;
        }

        analyzeExpr(bound);

        const EvaluatedType boundType = bound->getType();
        const bool countsPositions = boundType == EvaluatedType::Integer
                                  || boundType == EvaluatedType::Any
                                  || boundType == EvaluatedType::Null;

        if (!countsPositions) {
            throwError(fmt::format("A slice counts its bounds in integers, not '{}'",
                                   EvaluatedTypeName::value(boundType)),
                       expr);
        }

        dynamic = dynamic || bound->isDynamic();
        aggregate = aggregate || bound->isAggregate();
    }

    // Slicing an unknown value is unknown, as adding to one is: null[1..2] is null. The
    // elements are otherwise the ones the base holds, so the slice keeps its shape
    const EvaluatedType slicedType = baseType == EvaluatedType::Null ? EvaluatedType::Null
                                                                    : EvaluatedType::List;

    expr->setType(slicedType);
    expr->setListShape(base->getListShape());

    if (dynamic) {
        expr->setDynamic();
    }

    if (aggregate) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, slicedType));
}

void ExprAnalyzer::analyzeListExpr(ListExpr* expr) {
    analyzeListElements(expr, expr->getElements());
}

void ExprAnalyzer::analyzeListComprehensionExpr(ListComprehensionExpr* expr) {
    Expr* const source = expr->getSource();
    analyzeExpr(source);

    const EvaluatedType sourceType = source->getType();

    // A tagged cell names no type until a row is in hand, and a null iterates into a null
    // rather than into a type error - as an UNWIND of one emits no row
    const bool iteratesAList = sourceType == EvaluatedType::List
                            || sourceType == EvaluatedType::Any
                            || sourceType == EvaluatedType::Null;

    if (!iteratesAList) {
        throwError(fmt::format("A list comprehension iterates a list, not '{}'",
                               EvaluatedTypeName::value(sourceType)),
                   expr);
    }

    const std::string_view itemName = expr->getSymbol()->getName();

    // The comprehension names a new variable, as an UNWIND does, so a name already in
    // scope would put two of them under one name
    if (_ctxt->hasDecl(itemName)) {
        throwError(fmt::format("Variable '{}' is already declared", itemName), expr);
    }

    const ListShape& sourceShape = source->getListShape();

    // A null list holds no element, so the variable never reads as anything but null
    const EvaluatedType itemType = sourceType == EvaluatedType::Null ? EvaluatedType::Null
                                                                     : sourceShape.unwoundType();

    VarDecl* const itemDecl = _ctxt->getOrCreateNamedVariable(_ast, itemType, itemName);
    itemDecl->setIsUnwound(true);
    itemDecl->setListShape(sourceShape.unwound());

    expr->setDecl(itemDecl);

    Expr* const predicate = expr->getPredicate();
    if (predicate) {
        analyzeExpr(predicate);
    }

    Expr* const projection = expr->getProjection();
    if (projection) {
        analyzeExpr(projection);
    }

    _ctxt->dropVariable(itemName);

    if (predicate) {
        const EvaluatedType predicateType = predicate->getType();

        // A null WHERE holds for no element, as a null WHERE cuts every row. An element of a
        // list naming no type for its elements is read as a truth value row by row
        if (!namesATruthValue(predicateType)) {
            throwError("The WHERE of a list comprehension must be a boolean", predicate);
        }
    }

    const bool aggregatesTheBody = (predicate && predicate->isAggregate())
                                || (projection && projection->isAggregate());

    if (aggregatesTheBody) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of a "
                               "list comprehension: '{}' names one element, not a group",
                               itemName),
                   expr);
    }

    expr->setType(EvaluatedType::List);

    // Filtering leaves the shape the source has; a projection replaces the elements, so
    // the list gathers whatever it computes - what an UNWIND of the comprehension binds
    if (projection) {
        expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));
    } else {
        expr->setListShape(sourceShape);
    }

    // The elements are read row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    // An aggregated source aggregates the comprehension: the projection reduces the rows
    // to one, and the list is built over that one
    if (source->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeListPredicateExpr(ListPredicateExpr* expr) {
    ListComprehensionExpr* const comprehension = expr->getComprehension();
    analyzeExpr(comprehension);

    if (!comprehension->getPredicate()) {
        throwError("A list predicate needs a WHERE over its elements: all(x IN xs WHERE p(x))", expr);
    }

    expr->setType(EvaluatedType::Bool);
    expr->setDynamic();

    if (comprehension->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Bool));
}

template <typename FoldExpr>
void ExprAnalyzer::openFoldScope(FoldExpr* expr, std::string_view foldName) {
    Expr* const initialValue = expr->getInitialValue();
    Expr* const source = expr->getSource();
    analyzeExpr(initialValue);
    analyzeExpr(source);

    const EvaluatedType sourceType = source->getType();

    const bool iteratesAList = sourceType == EvaluatedType::List
                            || sourceType == EvaluatedType::Any
                            || sourceType == EvaluatedType::Null;

    if (!iteratesAList) {
        throwError(fmt::format("{} iterates a list, not '{}'", foldName, EvaluatedTypeName::value(sourceType)),
                   expr);
    }

    const std::string_view accumulatorName = expr->getAccumulatorSymbol()->getName();
    const std::string_view itemName = expr->getItemSymbol()->getName();

    for (const std::string_view name : {accumulatorName, itemName}) {
        if (_ctxt->hasDecl(name)) {
            throwError(fmt::format("Variable '{}' is already declared", name), expr);
        }
    }

    if (accumulatorName == itemName) {
        throwError(fmt::format("Variable '{}' is already declared", itemName), expr);
    }

    const ListShape& sourceShape = source->getListShape();
    const EvaluatedType itemType = sourceType == EvaluatedType::Null ? EvaluatedType::Null
                                                                     : sourceShape.unwoundType();

    VarDecl* const itemDecl = _ctxt->getOrCreateNamedVariable(_ast, itemType, itemName);
    itemDecl->setIsUnwound(true);
    itemDecl->setListShape(sourceShape.unwound());

    VarDecl* const accumulatorDecl = _ctxt->getOrCreateNamedVariable(_ast, initialValue->getType(), accumulatorName);
    accumulatorDecl->setExprType(initialValue->getExprType());

    expr->setItemDecl(itemDecl);
    expr->setAccumulatorDecl(accumulatorDecl);

    // The accumulator holds the initial value and then whatever the expression computed
    // from it, so its type is what both share. Reading it as the initial value's alone
    // would fold `acc + x` to null under `reduce(acc = null, ...)`
    Expr* const expression = expr->getExpression();

    while (true) {
        analyzeExpr(expression);

        const ExprType& accumulator = accumulatorDecl->getExprType();
        const EvaluatedType accumulatorCommonType = accumulatorDecl->getCommonType();
        const ListShape& accumulatorShape = accumulatorDecl->getListShape();

        ExprType widened;
        widened._type = accumulatedType(accumulator._type, expression->getType());
        widened._commonType = commonType(accumulatorCommonType, expression->getCommonType());
        widened._shape = ListShape::unified(accumulatorShape, expression->getListShape());

        const bool settled = widened._type == accumulator._type
                          && widened._commonType == accumulatorCommonType
                          && widened._shape.getLeafType() == accumulatorShape.getLeafType()
                          && widened._shape.getDepth() == accumulatorShape.getDepth();

        if (settled) {
            break;
        }

        accumulatorDecl->setExprType(widened);
        forgetAnalysis(expression);
    }
}

template <typename FoldExpr>
void ExprAnalyzer::closeFoldScope(FoldExpr* expr) {
    _ctxt->dropVariable(expr->getItemSymbol()->getName());
    _ctxt->dropVariable(expr->getAccumulatorSymbol()->getName());
}

void ExprAnalyzer::analyzeReduceExpr(ReduceExpr* expr) {
    openFoldScope(expr, "A reduce");
    closeFoldScope(expr);

    if (expr->getExpression()->isAggregate()) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of a "
                               "reduce: '{}' names one element, not a group",
                               expr->getItemSymbol()->getName()),
                   expr);
    }

    expr->setExprType(expr->getAccumulatorDecl()->getExprType());
    expr->setDynamic();

    if (expr->getInitialValue()->isAggregate() || expr->getSource()->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, expr->getType()));
}

void ExprAnalyzer::analyzeAllReduceExpr(AllReduceExpr* expr) {
    openFoldScope(expr, "An allReduce");

    Expr* const predicate = expr->getPredicate();
    analyzeExpr(predicate);

    closeFoldScope(expr);

    if (expr->getExpression()->isAggregate() || predicate->isAggregate()) {
        throwError(fmt::format("Aggregate functions may not be used over the elements of an "
                               "allReduce: '{}' names one element, not a group",
                               expr->getItemSymbol()->getName()),
                   expr);
    }

    if (!namesATruthValue(predicate->getType())) {
        throwError("The predicate of an allReduce must be a boolean", predicate);
    }

    expr->setType(EvaluatedType::Bool);
    expr->setDynamic();

    if (expr->getInitialValue()->isAggregate() || expr->getSource()->isAggregate()) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::Bool));
}

void ExprAnalyzer::analyzeMapProjectionExpr(MapProjectionExpr* expr) {
    SymbolExpr* const base = expr->getBase();
    analyzeExpr(base);

    Symbol* const baseSymbol = base->getSymbol();
    VarDecl* const baseDecl = base->getDecl();
    const EvaluatedType baseType = base->getType();

    // A tagged cell names no type until a row is in hand, and a null projects to null
    const bool projectsAnEntity = baseType == EvaluatedType::NodePattern
                               || baseType == EvaluatedType::EdgePattern;
    const bool projectsAMap = baseType == EvaluatedType::Map
                           || baseType == EvaluatedType::Any;
    const bool projectsComponents = baseType == EvaluatedType::DateTime
                                 || baseType == EvaluatedType::Duration
                                 || baseType == EvaluatedType::Point;
    const bool projectsNull = baseType == EvaluatedType::Null;

    if (!projectsAnEntity && !projectsAMap && !projectsComponents && !projectsNull) {
        throwError(fmt::format("A map projection reads a node, a relationship, a map, a datetime, a duration or a point, "
                               "and '{}' is '{}'",
                               baseSymbol->getName(),
                               EvaluatedTypeName::value(baseType)),
                   base);
    }

    bool aggregate = false;

    for (MapProjectionExpr::Item& item : expr->getItems()) {
        const bool readsAProperty = item._kind == MapProjectionExpr::ItemKind::Property;
        const bool readsEveryProperty = item._kind == MapProjectionExpr::ItemKind::AllProperties;

        if (readsEveryProperty && projectsComponents) {
            throwError(fmt::format("A map projection reads .* off a node, a relationship or a map, and '{}' is '{}'",
                                   baseSymbol->getName(),
                                   EvaluatedTypeName::value(baseType)),
                       base);
        }

        // `.key` reads what `v.key` reads, so it is analyzed as that read
        if (readsAProperty && !item._value) {
            QualifiedName* name = QualifiedName::create(_ast);
            name->addName(baseSymbol);
            name->addName(item._key);

            PropertyExpr* read = PropertyExpr::create(_ast, name);
            read->setEntityVarDecl(baseDecl);
            item._value = read;
        }

        if (!item._value) {
            continue;
        }

        analyzeExpr(item._value);
        aggregate = aggregate || item._value->isAggregate();
    }

    const EvaluatedType type = projectsNull ? EvaluatedType::Null : EvaluatedType::Map;
    expr->setType(type);
    expr->setDynamic();

    if (aggregate) {
        expr->setAggregate();
    }

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, type));
}

void ExprAnalyzer::forgetAnalysis(const Expr* expr) {
    std::vector<const Expr*> pending {expr};
    std::vector<const Expr*> children;

    while (!pending.empty()) {
        const Expr* const current = pending.back();
        pending.pop_back();

        _analyzedExprs.erase(current);

        children.clear();
        if (ExprChildren::collect(current, children)) {
            pending.insert(pending.end(), children.begin(), children.end());
        }
    }
}

void ExprAnalyzer::analyzePatternComprehensionExpr(PatternComprehensionExpr* expr) {
    const Pattern* pattern = expr->getPattern();

    // A name the pattern binds that the scope does not hold yet is the comprehension's
    // own: the WHERE and the projection read it, and nothing outside them does
    std::vector<std::string_view> ownVariables;
    std::vector<const EntityPattern*> ownEntities;
    std::vector<std::string_view> enclosingVariables;
    std::vector<std::pair<std::string_view, VarDecl*>> castSources;

    for (const PatternElement* element : pattern->elements()) {
        if (const Symbol* pathSymbol = element->getPathSymbol()) {
            ownVariables.push_back(pathSymbol->getName());
        }

        for (const EntityPattern* entity : element->getEntities()) {
            const Symbol* symbol = entity->getSymbol();
            if (!symbol) {
                continue;
            }

            const std::string_view name = symbol->getName();
            VarDecl* declared = _ctxt->getDecl(name);

            // A value of the scope known per row only is cast for the body alone: after the
            // comprehension the scope reads the value again
            if (declared && holdsAnEntityPerRow(declared->getType())) {
                castSources.emplace_back(name, declared);

                VarDecl* bound = entity->getDecl();
                if (bound && bound->getCastSource() == declared) {
                    _ctxt->declareAlias(name, bound);
                }
            }

            if (declared) {
                continue;
            }

            // A name the scope reads through to the one around it is that variable: the
            // pattern binds to it rather than to a new one
            if (VarDecl* enclosing = resolveVariable(symbol->getName())) {
                _ctxt->declareAlias(symbol->getName(), enclosing);
                enclosingVariables.push_back(symbol->getName());
                continue;
            }

            ownVariables.push_back(symbol->getName());
            ownEntities.push_back(entity);

            // A SET analyzes the expression it assigns twice, and the reads in the body
            // hold the declarations the first pass bound: binding the names back to them
            // leaves the second pass reading the variables the pattern already has, not
            // fresh ones the traversal knows nothing about.
            if (VarDecl* bound = entity->getDecl()) {
                _ctxt->declareAlias(symbol->getName(), bound);
            }
        }
    }

    _readAnalyzer->analyze(pattern);

    PatternComprehensionExpr::OwnDecls ownDecls;
    for (const EntityPattern* entity : ownEntities) {
        ownDecls.push_back(entity->getDecl());
    }

    for (const PatternElement* element : pattern->elements()) {
        if (const VarDecl* pathDecl = element->getPathDecl()) {
            ownDecls.push_back(pathDecl);
        }
    }

    expr->setOwnDecls(ownDecls);

    Expr* const projection = expr->getProjection();
    analyzeExpr(projection);

    for (const std::string_view name : ownVariables) {
        _ctxt->dropVariable(name);
    }

    for (const std::string_view name : enclosingVariables) {
        _ctxt->dropVariable(name);
    }

    for (const auto& [name, source] : castSources) {
        _ctxt->dropCastVariable(name, source);
    }

    if (projection->isAggregate()) {
        throwError("Aggregate functions may not be used over the matches of a pattern "
                   "comprehension: the pattern names one match, not a group",
                   expr);
    }

    expr->setType(EvaluatedType::List);
    expr->setListShape(ListShape::collecting(projection->getType(), projection->getListShape()));

    // The pattern is matched row by row, so the list is never the compile-time value an
    // argument declared constant takes
    expr->setDynamic();

    expr->setExprVarDecl(_ctxt->createUnnamedVariable(_ast, EvaluatedType::List));
}

void ExprAnalyzer::analyzeListElements(Expr* expr, std::span<Expr* const> elements) {
    for (Expr* element : elements) {
        analyzeExpr(element);

        // An element is an expression of its own, so its flags are the list's: the n of
        // [n, m] reads a row, and so does the {age: n.age} of ORDER BY [{age: n.age}]
        if (element->isDynamic()) {
            expr->setDynamic();
        }

        if (element->isAggregate()) {
            expr->setAggregate();
        }
    }

    expr->setListShape(sharedListShape(elements));
}

void ExprAnalyzer::analyzeMapEntries(Expr* expr, const MapLiteral* map) {
    // The keys of a map are symbols written in the query, so only its values can make it
    // vary or aggregate
    for (const auto& [key, value] : *map) {
        analyzeExpr(value);

        if (value->isDynamic()) {
            expr->setDynamic();
        }

        if (value->isAggregate()) {
            expr->setAggregate();
        }
    }
}
