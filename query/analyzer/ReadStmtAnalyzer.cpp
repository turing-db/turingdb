#include "ReadStmtAnalyzer.h"

#include <string_view>

#include "AnalyzeException.h"
#include "ExprAnalyzer.h"
#include "FunctionInvocation.h"
#include "SymbolChain.h"
#include "YieldClause.h"
#include "YieldItems.h"
#include "decl/EvaluatedType.h"
#include "metadata/GraphMetadata.h"

#include "DiagnosticsManager.h"
#include "CypherAST.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"
#include "decl/PatternData.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ShortestPathStmt.h"
#include "stmt/Skip.h"
#include "stmt/Limit.h"
#include "stmt/MatchStmt.h"
#include "stmt/CallStmt.h"
#include "stmt/LoadCSVStmt.h"
#include "stmt/UnwindStmt.h"
#include "stmt/VectorSearchStmt.h"
#include "QualifiedName.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Symbol.h"
#include "Literal.h"
#include "WhereClause.h"

#include "expr/Expr.h"
#include "expr/BinaryExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/EntityTypeExpr.h"
#include "expr/PathExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StringExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/ListExpr.h"

#include "QuantifiedPath.h"
#include "ProcedureLookup.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

ReadStmtAnalyzer::ReadStmtAnalyzer(CypherAST* ast, GraphView graphView)
    : _ast(ast),
    _graphView(graphView),
    _graphMetadata(graphView.metadata())
{
}

ReadStmtAnalyzer::~ReadStmtAnalyzer() {
}

void ReadStmtAnalyzer::analyze(Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            analyze(static_cast<const MatchStmt*>(stmt));
        break;

        case Stmt::Kind::CALL:
            analyze(static_cast<const CallStmt*>(stmt));
        break;

        case Stmt::Kind::LOAD_CSV:
            analyze(static_cast<LoadCSVStmt*>(stmt));
        break;

        case Stmt::Kind::VECTOR_SEARCH:
            analyze(static_cast<const VectorSearchStmt*>(stmt));
        break;

        case Stmt::Kind::SHORTESTPATH:
            analyze(static_cast<ShortestPathStmt*>(stmt));
        break;

        case Stmt::Kind::UNWIND:
            analyze(static_cast<UnwindStmt*>(stmt));
        break;

        case Stmt::Kind::CREATE:
        case Stmt::Kind::SET:
        case Stmt::Kind::DELETE:
        case Stmt::Kind::RETURN:
        case Stmt::Kind::WITH:
            throw FatalException("Attempted to evaluate invalid read statement.");
        break;
    }
}

void ReadStmtAnalyzer::analyze(const MatchStmt* matchSt) {
    if (matchSt->isOptional()) {
        throwError("OPTIONAL MATCH not supported", matchSt);
    }

    const Pattern* pattern = matchSt->getPattern();
    if (!pattern) {
        throwError("MATCH statement must have a pattern", matchSt);
    }

    analyze(pattern);

    if (matchSt->hasOrderBy()) {
        analyze(matchSt->getOrderBy());
    }

    if (matchSt->hasSkip()) {
        analyze(matchSt->getSkip());
    }

    if (matchSt->hasLimit()) {
        analyze(matchSt->getLimit());
    }
}

void ReadStmtAnalyzer::analyze(const CallStmt* callStmt) {
    // Step 1. Check OPTIONAL keyword
    if (callStmt->isOptional()) {
        throwError("OPTIONAL CALL not supported", callStmt);
    }

    // Step 2. Analyze function invocation
    FunctionInvocationExpr* funcExpr = callStmt->getFunc();
    if (!funcExpr) [[unlikely]] {
        throwError("CALL statement must have a function invocation", callStmt);
    }

    _exprAnalyzer->analyzeFuncInvocExpr(funcExpr, _ast->getProcedureLookup());

    const FunctionInvocation* func = funcExpr->getFunctionInvocation();
    const FunctionSignature* signature = func->getSignature();

    if (!signature->isProcedure()) {
        throwError(fmt::format("Function '{} is not a database procedure'",
                               signature->getFullName()),
                   callStmt);
    }

    // Step 3. Analyze YIELD clause
    YieldClause* yield = callStmt->getYield();
    if (yield == nullptr) {
        if (!callStmt->isStandaloneCall()) {
            throwError("Procedure call inside a query requires to name the "
                       "return items explicitly with a YIELD clause",
                       callStmt);
        }
        return;
    }

    // YIELD * names every return value the procedure declares. A standalone call needs no
    // declarations for them, so its items stay unnamed for the code generator to spell out.
    if (yield->getItems() == nullptr) {
        if (callStmt->isStandaloneCall()) {
            return;
        }

        yieldEveryReturnValue(*signature, yield);
    }

    analyze(*func, yield);
}

void ReadStmtAnalyzer::yieldEveryReturnValue(const FunctionSignature& signature, YieldClause* yield) {
    YieldItems* items = YieldItems::create(_ast);
    for (const FunctionReturnType& returnType : signature.returnTypes()) {
        Symbol* symbol = Symbol::create(_ast, returnType.getName());
        items->add(SymbolExpr::create(_ast, symbol));
    }

    yield->setItems(items);
}

void ReadStmtAnalyzer::analyze(LoadCSVStmt* loadCSV) {
    Symbol* alias = loadCSV->getAlias();
    if (!alias) {
        throwError("LOAD CSV must have an alias", loadCSV);
    }

    if (_ctxt->hasDecl(alias->getName())) {
        throwError(fmt::format("Variable '{}' already declared",
                               alias->getName()),
                   loadCSV);
    }

    VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast,
                                                     EvaluatedType::StringTable,
                                                     alias->getName());

    loadCSV->setAliasDecl(decl);

    _exprAnalyzer->registerCSVSource(decl, loadCSV);
}

void ReadStmtAnalyzer::analyze(const FunctionInvocation& func, const YieldClause* yield) {
    bioassert(func.getSignature(), "Analyzed a yield function that has no signature");

    FunctionSignature* signature = func.getSignature();
    YieldItems* yieldItems = yield->getItems();

    // Step 2. Create the decls for the yield items
    for (SymbolExpr* yieldItemExpr : *yieldItems) {
        Symbol* yieldItem = yieldItemExpr->getSymbol();

        if (_ctxt->hasDecl(yieldItem->getName())) {
            throwError(fmt::format("Variable '{}' already declared", yieldItem->getName()), yieldItemExpr);
        }

        VarDecl* decl = nullptr;

        // Step 3. Find the item in the return values of the function
        for (const FunctionReturnType& returnItem : signature->returnTypes()) {
            if (returnItem.getName() == yieldItem->getOriginalName()) {
                bioassert(!returnItem.getName().empty(), "Procedure return item has empty name");
                decl = _ctxt->getOrCreateNamedVariable(_ast, returnItem.getType(), yieldItem->getName());
                yieldItemExpr->setDecl(decl);
                break;
            }
        }

        if (decl == nullptr) {
            throwError(fmt::format("Procedure '{}' does not return item '{}'",
                                   signature->getFullName(), yieldItem->getOriginalName()),
                       yieldItemExpr);
        }

        yieldItemExpr->setExprVarDecl(decl);
    }

    // Step 4. Analyze WHERE clause on YIELD items
    analyzeYieldFilter(yieldItems);
}

void ReadStmtAnalyzer::analyzeYieldFilter(const YieldItems* yieldItems) {
    const WhereClause* where = yieldItems->getWhereClause();
    if (!where) {
        return;
    }

    Expr* whereExpr = where->getExpr();
    _exprAnalyzer->analyzeRootExpr(whereExpr);

    if (whereExpr->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", where);
    }

    if (whereExpr->getType() != EvaluatedType::Bool) {
        throwError("WHERE expression must be a boolean", where);
    }
}

void ReadStmtAnalyzer::analyze(OrderBy* orderBySt) {
    for (OrderByItem* item : orderBySt->getItems()) {
        Expr* expr = item->getExpr();
        _exprAnalyzer->analyzeRootExpr(expr);

        if (expr->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", orderBySt);
        }
    }
}

void ReadStmtAnalyzer::analyze(Skip* skip) {
    Expr* expr = skip->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->isDynamic()) {
        throwError("SKIP expression must be a value that can be evaluated at compile time", skip);
    }

    if (expr->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", skip);
    }

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("SKIP expression must be an integer", skip);
    }
}

void ReadStmtAnalyzer::analyze(Limit* limit) {
    Expr* expr = limit->getExpr();
    _exprAnalyzer->analyzeRootExpr(expr);

    if (expr->isDynamic()) {
        throwError("LIMIT expression must be a value that can be evaluated at compile time", limit);
    }

    if (expr->isAggregate()) {
        throwError("Invalid use of aggregate expression in this context", limit);
    }

    if (expr->getType() != EvaluatedType::Integer) {
        throwError("LIMIT expression must be an integer", limit);
    }
}

void ReadStmtAnalyzer::analyze(const Pattern* pattern) {
    for (const PatternElement* element : pattern->elements()) {
        analyze(element);
    }

    if (const WhereClause* where = pattern->getWhere()) {
        Expr* whereExpr = where->getExpr();
        _exprAnalyzer->analyzeRootExpr(whereExpr);

        if (whereExpr->isAggregate()) {
            throwError("Invalid use of aggregate expression in this context", pattern);
        }

        if (whereExpr->getType() != EvaluatedType::Bool) {
            throwError("WHERE expression must be a boolean", pattern);
        }
    }
}

void ReadStmtAnalyzer::analyze(const PatternElement* element) {
    const auto& entities = element->getEntities();

    for (EntityPattern* entity : entities) {
        if (NodePattern* node = dynamic_cast<NodePattern*>(entity)) {
            analyze(node);
        } else if (EdgePattern* edge = dynamic_cast<EdgePattern*>(entity)) {
            analyze(edge);
        } else {
            throwError("Unsupported pattern entity type", entity);
        }
    }
}

void ReadStmtAnalyzer::analyze(NodePattern* nodePattern) {
    VarDecl* decl = nullptr;

    if (Symbol* symbol = nodePattern->getSymbol()) {
        decl = _ctxt->getOrCreateNamedVariable(_ast,
                                               EvaluatedType::NodePattern,
                                               symbol->getName());
        nodePattern->setDecl(decl);
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::NodePattern);
        nodePattern->setDecl(decl);
    }

    NodePatternData* data = NodePatternData::create(_ast);
    nodePattern->setData(data);

    const auto& labels = nodePattern->labels();
    if (labels && !labels->empty()) {
        const LabelMap& labelMap = _graphMetadata.labels();

        for (const Symbol* label : *labels) {
            const std::optional<LabelID> labelID = labelMap.get(label->getName());
            if (!labelID) {
                throwError(fmt::format("Unknown label: {}", label->getName()), nodePattern);
            }

            data->addLabelConstraint(label->getName());
        }
    }

    const MapLiteral* properties = nodePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", nodePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), nodePattern);
            }

            if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate node property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           nodePattern);
            }

            // Create a dummy Expr that represents the predicate evaluation
            Symbol* varSymbol = Symbol::create(_ast, decl->getName());
            QualifiedName* fullName = QualifiedName::create(_ast);

            fullName->addName(varSymbol);
            fullName->addName(propName);

            PropertyExpr* propExpr = PropertyExpr::create(_ast, fullName);
            BinaryExpr* predExpr = BinaryExpr::create(_ast, BinaryOperator::Equal, propExpr, expr);
            _exprAnalyzer->analyzeRootExpr(predExpr);

            data->addExprConstraint(propName->getName(), propType->_valueType, predExpr);
        }
    }
}

void ReadStmtAnalyzer::analyze(EdgePattern* edgePattern) {
    VarDecl* decl = nullptr;

    if (Symbol* symbol = edgePattern->getSymbol()) {
        decl = _ctxt->getOrCreateNamedVariable(_ast,
                                               EvaluatedType::EdgePattern,
                                               symbol->getName());
        edgePattern->setDecl(decl);
    } else {
        decl = _ctxt->createUnnamedVariable(_ast, EvaluatedType::EdgePattern);
        edgePattern->setDecl(decl);
    }

    EdgePatternData* data = EdgePatternData::create(_ast);
    edgePattern->setData(data);

    const auto& types = edgePattern->types();
    if (types && !types->empty()) {
        const EdgeTypeMap& edgeTypeMap = _graphMetadata.edgeTypes();

        for (const Symbol* edgeTypeSymbol : *types) {
            const std::optional<EdgeTypeID> etID = edgeTypeMap.get(edgeTypeSymbol->getName());
            if (!etID) {
                throwError(fmt::format("Unknown edge type: {}",
                                       edgeTypeSymbol->getName()),
                           edgePattern);
            }

            data->addEdgeTypeConstraint(edgeTypeSymbol->getName());
        }
    }

    const MapLiteral* properties = edgePattern->getProperties();
    if (properties) {
        const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();

        for (const auto& [propName, expr] : *properties) {
            _exprAnalyzer->analyzeRootExpr(expr);

            if (expr->isAggregate()) {
                throwError("Invalid use of aggregate expression in this context", edgePattern);
            }

            const std::optional<PropertyType> propType = propTypeMap.get(propName->getName());
            if (!propType) {
                throwError(fmt::format("Unknown property: {}", propName->getName()), edgePattern);
            }

            if (!ExprAnalyzer::propTypeCompatible(propType->_valueType, expr->getType())) {
                throwError(fmt::format("Cannot evaluate edge property: types '{}' and '{}' are incompatible",
                                       ValueTypeName::value(propType->_valueType),
                                       EvaluatedTypeName::value(expr->getType())),
                           edgePattern);
            }

            // Create a dummy Expr that represents the predicate evaluation
            Symbol* varSymbol = Symbol::create(_ast, decl->getName());
            QualifiedName* fullName = QualifiedName::create(_ast);

            fullName->addName(varSymbol);
            fullName->addName(propName);

            PropertyExpr* propExpr = PropertyExpr::create(_ast, fullName);
            BinaryExpr* predExpr = BinaryExpr::create(_ast, BinaryOperator::Equal, propExpr, expr);
            _exprAnalyzer->analyzeRootExpr(predExpr);

            data->addExprConstraint(propName->getName(), propType->_valueType, predExpr);
        }
    }

    // Validate QuantifiedPath
    const QuantifiedPath* qp = edgePattern->getQuantifiedPath();
    if (qp) {
        const int64_t lhs = qp->getLhs();
        const int64_t rhs = qp->getRhs();

        if (lhs < 0) {
            throwError("Variable-length path minimum hops must be greater than or equal to 0",
                       edgePattern);
        }

        if (rhs < 1) {
            throwError("Variable-length path maximum hops must be greater than or equal to 1",
                       edgePattern);
        }

        if (!qp->isRhsUnbounded()) {
            if (rhs < lhs) {
                throwError("Variable-length path maximum hops must be "
                           "greater than or equal to minimum hops",
                           edgePattern);
            }
        }

        const auto& types = edgePattern->types();
        if (types && !types->empty()) {
            throwError("Edge type filters are not supported with "
                       "variable-length paths yet",
                       edgePattern);
        }

        if (properties) {
            throwError("Edge property filters are not supported with "
                       "variable-length paths yet",
                       edgePattern);
        }
    }
}

void ReadStmtAnalyzer::analyze(const VectorSearchStmt* stmt) {
    // Validate K > 0
    if (stmt->getK() == 0) {
        throwError("VECTOR SEARCH k value must be greater than 0", stmt);
    }

    // Validate vector has elements
    const EmbeddingLiteral* queryVector = stmt->getQueryVector();
    if (!queryVector || queryVector->getValue().empty()) {
        throwError("VECTOR SEARCH query vector cannot be empty", stmt);
    }

    // Process YIELD clause - create VarDecl for 'ids' variable
    const YieldClause* yield = stmt->getYield();
    if (!yield) {
        throwError("VECTOR SEARCH requires YIELD clause", stmt);
    }

    const YieldItems* yieldItems = yield->getItems();
    if (!yieldItems || yieldItems->getItems().empty()) {
        throwError("VECTOR SEARCH YIELD clause cannot be empty", stmt);
    }

    for (SymbolExpr* yieldItemExpr : *yieldItems) {
        const Symbol* yieldItem = yieldItemExpr->getSymbol();

        // The statement's own name for the value picks which of the two it is; the symbol's
        // name is what the query calls it, which is the alias when the YIELD renamed it. So
        // two searches in one query yield under names of their own rather than colliding.
        const std::string_view yieldedValue = yieldItem->getOriginalName();
        const std::string_view yieldName = yieldItem->getName();

        EvaluatedType yieldType = EvaluatedType::Invalid;
        if (yieldedValue == "ids") {
            // The MLIR engine reports each neighbour as the node the index holds it under,
            // so a pattern can walk out of the yielded variable and an equality can compare
            // a matched node to it. The pipeline reports the raw ID instead.
            yieldType = _isV3 ? EvaluatedType::NodePattern : EvaluatedType::Integer;
        } else if (yieldedValue == "score") {
            yieldType = EvaluatedType::Double;
        } else {
            throwError(fmt::format("VECTOR SEARCH only supports YIELD ids and score, got '{}'",
                                   yieldedValue), stmt);
        }

        if (_ctxt->hasDecl(yieldName)) {
            throwError(fmt::format("Variable '{}' already declared", yieldName),
                       yieldItemExpr);
        }

        VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast, yieldType, yieldName);
        yieldItemExpr->setDecl(decl);
        yieldItemExpr->setExprVarDecl(decl);
    }

    analyzeYieldFilter(yieldItems);
}

void ReadStmtAnalyzer::analyze(ShortestPathStmt* spSt) {
    const PropertyTypeMap& propTypeMap = _graphMetadata.propTypes();
    const std::string_view propName = spSt->getEdgeProperty()->getName();

    const std::optional<PropertyType> propType = propTypeMap.get(propName);
    if (!propType) {
        throwError(fmt::format("Unknown property: {}", propName));
    }

    const Symbol* source = spSt->getSource();
    const Symbol* target = spSt->getTarget();
    bioassert(source && target, "Null endpoints.");

    spSt->setSourceDecl(resolveShortestPathEndpoint(source, spSt));
    spSt->setTargetDecl(resolveShortestPathEndpoint(target, spSt));

    const Symbol* distVar = spSt->getDistVar();
    const Symbol* pathVar = spSt->getPathVar();
    bioassert(distVar && pathVar, "Null variables.");

    const ValueType weightPropType = propType->_valueType;
    const auto maybeEvalType = toEvaluatedType(weightPropType);
    bioassert(maybeEvalType.has_value(), "Invalid value type.");
    const EvaluatedType evalType = maybeEvalType.value();

    const std::string_view distName = distVar->getName();
    const std::string_view pathName = pathVar->getName();

    VarDecl* distDecl = _ctxt->getOrCreateNamedVariable(_ast, evalType, distName);
    VarDecl* pathDecl = _ctxt->getOrCreateNamedVariable(_ast, EvaluatedType::GraphPath, pathName);

    spSt->setDistDecl(distDecl);
    spSt->setPathDecl(pathDecl);
}

VarDecl* ReadStmtAnalyzer::resolveShortestPathEndpoint(const Symbol* endpoint, const ShortestPathStmt* spSt) const {
    const std::string_view name = endpoint->getName();

    VarDecl* decl = _ctxt->getDecl(name);
    if (!decl) {
        throwError(fmt::format("Variable '{}' not found", name), spSt);
    }

    const EvaluatedType type = decl->getType();
    if (type != EvaluatedType::NodePattern) {
        throwError(fmt::format("SHORTESTPATH endpoint '{}' must be a node, but is {} instead",
                               name,
                               EvaluatedTypeName::value(type)),
                   spSt);
    }

    return decl;
}

void ReadStmtAnalyzer::analyze(UnwindStmt* unwind) {
    const Expr* arg = unwind->arg();
    bioassert(arg, "Invalid argument");

    {
        const Expr::Kind argKind = arg->getKind();
        const bool isLiteral = argKind == Expr::Kind::LITERAL;

        if (!isLiteral) {
            throwError("Non-literal UNWIND expressions are not yet supported.", arg);
        }
    }

    const auto* litArg = static_cast<const LiteralExpr*>(arg);

    const Literal* lit = litArg->getLiteral();
    bioassert(lit, "Invalid literal.");

    const Literal::Kind litKind = lit->getKind();
    const bool isList = litKind == Literal::Kind::LIST;

    /// Non-lists can be unwound, it just turns the argument into a singleton list
    if (!isList) {
        // TODO: Implement
        throwError("Non-list arguments to UNWIND are not yet supported", litArg);
    }

    const auto* list = static_cast<const ListLiteral*>(lit);
    const ListLiteral::Items& items = list->items();
    for (Expr* ele : items) {
        _exprAnalyzer->analyzeExpr(ele);
    }

    EvaluatedType itemType = EvaluatedType::ListItem;
    if (!items.empty()) {
        // Check for homogeneity: update evaluated type if found
        const auto differingType = [](const Expr* a, const Expr* b) {
            return a->getType() != b->getType();
        };

        const auto typeIt = std::ranges::adjacent_find(items, differingType);
        const bool homogeneous = typeIt == end(items);

        if (homogeneous) {
            // List is homogeneous and non empty: perform type restriction
            const Expr* item = items.front();
            const EvaluatedType homogeneity = item->getType();
            // Could be a list of lists: there is no ValueType::List -> do not treat as
            // homogeneous
            const bool isValueType = convertibleToValueType(homogeneity);

            const bool canBeHomogeneous = isValueType;

            if (canBeHomogeneous) {
                itemType = homogeneity;
            }
        }
    }

    const Symbol* symbol = unwind->symbol();
    bioassert(symbol, "Invalid symbol.");

    const std::string_view symName = symbol->getName();

    // UNWIND names a new variable, so a name already in scope would put two of them under
    // one name - the list's rows and whatever that name already holds
    if (_ctxt->hasDecl(symName)) {
        throwError(fmt::format("Variable '{}' is already declared", symName), unwind);
    }

    const VarDecl* decl = _ctxt->getOrCreateNamedVariable(_ast, itemType, symName);
    unwind->setDecl(decl);
}

void ReadStmtAnalyzer::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw AnalyzeException(std::move(errorStr));
}
