#include "CypherAnalyzer.h"

#include <string_view>
#include <vector>

#include "CypherAST.h"
#include "EntityPattern.h"
#include "Literal.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "Projection.h"
#include "WhereClause.h"
#include "decl/DeclContext.h"
#include "expr/EntityTypeExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChildren.h"
#include "expr/ListComprehensionExpr.h"
#include "expr/PatternComprehensionExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/ReduceExpr.h"
#include "expr/AllReduceExpr.h"
#include "decl/VarDecl.h"
#include "expr/SymbolExpr.h"
#include "expr/FunctionInvocationExpr.h"
#include "FunctionInvocation.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"

#include "expr/ExistsExpr.h"
#include "expr/CountSubqueryExpr.h"
#include "expr/CollectSubqueryExpr.h"

using namespace db;

namespace {

const FunctionSignature* aggregateSignatureOf(const Expr* expr) {
    if (expr->getKind() != Expr::Kind::FUNCTION_INVOCATION) {
        return nullptr;
    }

    const FunctionInvocationExpr* call = static_cast<const FunctionInvocationExpr*>(expr);
    const FunctionInvocation* invocation = call->getFunctionInvocation();
    const FunctionSignature* signature = invocation->getSignature();

    if (!signature || !signature->isAggregate()) {
        return nullptr;
    }

    return signature;
}

// The variables @param expr binds per element for its child @param child to read: a list
// comprehension's element, and a fold's element and accumulator, which neither the list
// iterated nor the initial value sees
void collectElementDecls(const Expr* expr, const Expr* child, std::vector<const VarDecl*>& decls) {
    const Expr::Kind kind = expr->getKind();

    if (kind == Expr::Kind::LIST_COMPREHENSION) {
        const ListComprehensionExpr* comprehension = static_cast<const ListComprehensionExpr*>(expr);

        if (child != comprehension->getSource()) {
            decls.push_back(comprehension->getDecl());
        }
    } else if (kind == Expr::Kind::REDUCE) {
        const ReduceExpr* reduce = static_cast<const ReduceExpr*>(expr);

        if (child == reduce->getExpression()) {
            decls.push_back(reduce->getItemDecl());
            decls.push_back(reduce->getAccumulatorDecl());
        }
    } else if (kind == Expr::Kind::ALL_REDUCE) {
        const AllReduceExpr* allReduce = static_cast<const AllReduceExpr*>(expr);

        const bool readsTheFold = child == allReduce->getExpression() || child == allReduce->getPredicate();
        if (readsTheFold) {
            decls.push_back(allReduce->getItemDecl());
            decls.push_back(allReduce->getAccumulatorDecl());
        }
    }
}

}

void CypherAnalyzer::analyzeDistinct(const Projection* projection, bool isAggregate) const {
    // An aggregating projection emits one row per group, so no two of its rows are equal
    // and the dedup drops nothing: its keys answer to analyzeAggregateOrderBy instead
    if (isAggregate || !projection->hasOrderBy()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        const Expr* keyExpr = item->getExpr();

        // The dedup keys its rows on the columns the projection carries, so a key over
        // those holds one value per row it kept - the test a grouping key answers too
        if (!isGroupWise(keyExpr, projection)) {
            throwError("ORDER BY with DISTINCT may only order by expressions over the "
                       "returned columns.",
                       keyExpr);
        }
    }
}

void CypherAnalyzer::analyzeAggregateOrderBy(const Projection* projection) const {
    if (!projection->hasOrderBy()) {
        return;
    }

    const OrderBy* orderBy = projection->getOrderBy();

    for (const OrderByItem* item : orderBy->getItems()) {
        const Expr* keyExpr = item->getExpr();

        if (!isGroupWise(keyExpr, projection)) {
            throwError("ORDER BY with an aggregate may only order by expressions over the "
                       "returned columns.",
                       keyExpr);
        }
    }
}

void CypherAnalyzer::throwOnOrderByAggregate(const Projection* projection, const Stmt* clause) const {
    const std::string_view clauseName = clause->getKind() == Stmt::Kind::WITH ? "WITH" : "RETURN";

    for (const OrderByItem* item : projection->getOrderBy()->getItems()) {
        const Expr* keyExpr = item->getExpr();

        if (keyExpr->isAggregate()) {
            throwError(fmt::format("Cannot use aggregation in ORDER BY if there are no aggregate "
                                   "expressions in the preceding {}",
                                   clauseName),
                       keyExpr);
        }
    }
}

void CypherAnalyzer::analyzeAggregateItems(const Projection* projection) const {
    for (const Projection::ReturnItem& returnItem : projection->items()) {
        const auto* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr || !(*exprPtr)->isAggregate()) {
            continue;
        }

        DeclSet elements;
        throwOnImplicitGroupingKey(*exprPtr, projection, elements);
    }
}

void CypherAnalyzer::throwOnImplicitGroupingKey(const Expr* expr,
                                                const Projection* projection,
                                                DeclSet& elements) const {
    if (aggregateSignatureOf(expr)) {
        return;
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return;
    }

    std::vector<const VarDecl*> elementDecls;
    for (const Expr* child : children) {
        elementDecls.clear();
        collectElementDecls(expr, child, elementDecls);
        elements.insert(elementDecls.begin(), elementDecls.end());

        if (child->isAggregate()) {
            throwOnImplicitGroupingKey(child, projection, elements);
        } else if (!isGroupWise(child, projection, elements)) {
            throwError("An expression beside an aggregate may only read the grouping keys: "
                       "return this one as a key of its own, or aggregate it.",
                       child);
        }

        for (const VarDecl* decl : elementDecls) {
            elements.erase(decl);
        }
    }
}

bool CypherAnalyzer::isGroupWise(const Expr* expr, const Projection* projection) const {
    DeclSet elements;

    return isGroupWise(expr, projection, elements);
}

// @param elements holds the variables the list comprehensions above this expression bound.
// Each is bound per element of its own source rather than per matched row, so it holds one
// value wherever it is read and orders the groups as a constant does.
bool CypherAnalyzer::isGroupWise(const Expr* expr,
                                 const Projection* projection,
                                 DeclSet& elements) const {
    if (!expr) {
        return true;
    }

    // A constant holds the same value in every matched row, so it holds one per group too
    if (!expr->isDynamic()) {
        return true;
    }

    // An aggregate reduces its group to one value, which is what a group-wise key is: a
    // count of the group orders the groups whether or not the projection returns it
    if (aggregateSignatureOf(expr)) {
        return true;
    }

    // A grouping key holds one value per group by construction, and so does the alias of
    // one: the projection carries that column, at whatever depth of the key it is read
    if (projection->hasItem(expr)) {
        return true;
    }

    const Expr::Kind kind = expr->getKind();

    if (kind == Expr::Kind::PROPERTY) {
        const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
        const VarDecl* const entityDecl = property->getEntityVarDecl();

        return elements.contains(entityDecl) || projection->hasItemDecl(entityDecl);
    } else if (kind == Expr::Kind::ENTITY_TYPES) {
        const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);

        if (const Expr* operand = entityType->getOperand()) {
            return isGroupWise(operand, projection, elements);
        }

        const VarDecl* const entityDecl = entityType->getEntityVarDecl();

        return elements.contains(entityDecl) || projection->hasItemDecl(entityDecl);
    } else if (kind == Expr::Kind::SYMBOL) {
        // A returned variable, and the alias of a returned item, are both matched above:
        // a symbol reaching here names a variable the grouping consumed, unless a
        // comprehension over this key bound it
        const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);

        return elements.contains(symbol->getDecl());
    } else if (kind == Expr::Kind::PATTERN_COMPREHENSION) {
        const PatternComprehensionExpr* comprehension = static_cast<const PatternComprehensionExpr*>(expr);

        return isGroupWise(comprehension, projection, elements);
    } else if (kind == Expr::Kind::COUNT_SUBQUERY) {
        const CountSubqueryExpr* count = static_cast<const CountSubqueryExpr*>(expr);

        return readsGroupWiseVariables(count->getBody(), projection, elements);
    } else if (kind == Expr::Kind::COLLECT_SUBQUERY) {
        const CollectSubqueryExpr* collect = static_cast<const CollectSubqueryExpr*>(expr);

        return readsGroupWiseVariables(collect->getBody(), projection, elements);
    } else if (kind == Expr::Kind::EXISTS) {
        const ExistsExpr* exists = static_cast<const ExistsExpr*>(expr);

        return readsGroupWiseVariables(exists->getBody(), projection, elements);
    }

    std::vector<const Expr*> children;
    if (!ExprChildren::collect(expr, children)) {
        return false;
    }

    std::vector<const VarDecl*> elementDecls;
    for (const Expr* child : children) {
        elementDecls.clear();
        collectElementDecls(expr, child, elementDecls);
        elements.insert(elementDecls.begin(), elementDecls.end());

        const bool childIsGroupWise = isGroupWise(child, projection, elements);

        for (const VarDecl* decl : elementDecls) {
            elements.erase(decl);
        }

        if (!childIsGroupWise) {
            return false;
        }
    }

    return true;
}

// A pattern comprehension reduces the matches of its pattern to one list, so it holds one
// value per group wherever the rows it is matched on do: the variables it joins onto have
// to be grouping keys, and the ones it binds of its own stand as an element does.
bool CypherAnalyzer::isGroupWise(const PatternComprehensionExpr* comprehension,
                                 const Projection* projection,
                                 DeclSet& elements) const {
    const PatternComprehensionExpr::OwnDecls& ownDecls = comprehension->getOwnDecls();
    elements.insert(ownDecls.begin(), ownDecls.end());

    const Pattern* const pattern = comprehension->getPattern();
    const WhereClause* const where = pattern->getWhere();
    const Expr* const predicate = where ? where->getExpr() : nullptr;

    const bool patternIsGroupWise = joinsGroupWiseVariables(pattern, projection, elements);
    const bool predicateIsGroupWise = isGroupWise(predicate, projection, elements);
    const bool projectionIsGroupWise = isGroupWise(comprehension->getProjection(), projection, elements);

    for (const VarDecl* const decl : ownDecls) {
        elements.erase(decl);
    }

    return patternIsGroupWise && predicateIsGroupWise && projectionIsGroupWise;
}

bool CypherAnalyzer::joinsGroupWiseVariables(const Pattern* pattern,
                                             const Projection* projection,
                                             DeclSet& elements) const {
    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const MapLiteral* const properties = entity->getProperties();

            if (properties) {
                for (const auto& [key, value] : *properties) {
                    if (!isGroupWise(value, projection, elements)) {
                        return false;
                    }
                }
            }

            // An entity the pattern did not name is matched here and read nowhere else, so
            // no group can disagree on what it holds
            if (!entity->getSymbol()) {
                continue;
            }

            const VarDecl* const decl = entity->getDecl();

            if (!elements.contains(decl) && !projection->hasItemDecl(decl)) {
                return false;
            }
        }
    }

    return true;
}

bool CypherAnalyzer::readsGroupWiseVariables(const RegularQuery* body,
                                             const Projection* projection,
                                             const DeclSet& elements) const {
    DeclSet read;
    collectReadDecls(read);

    std::vector<const RegularQuery*> queries;
    RegularQuery::collectQueries(body, queries);

    for (const RegularQuery* query : queries) {
        for (const VarDecl* decl : query->getDeclContext()->decls()) {
            const auto sourceIt = _declSources.find(decl);
            const bool readsAnImport = sourceIt != _declSources.end() && read.contains(decl);

            if (!readsAnImport) {
                continue;
            }

            const VarDecl* source = sourceIt->second;

            if (!elements.contains(source) && !projection->hasItemDecl(source)) {
                return false;
            }
        }
    }

    return true;
}

void CypherAnalyzer::collectReadDecls(DeclSet& read) const {
    std::vector<const VarDecl*> pending;

    for (const Expr* expr : _ast->getExpressions()) {
        const Expr::Kind kind = expr->getKind();

        if (kind == Expr::Kind::SYMBOL) {
            const SymbolExpr* symbol = static_cast<const SymbolExpr*>(expr);
            pending.push_back(symbol->getDecl());
        } else if (kind == Expr::Kind::PROPERTY) {
            const PropertyExpr* property = static_cast<const PropertyExpr*>(expr);
            pending.push_back(property->getEntityVarDecl());
        } else if (kind == Expr::Kind::ENTITY_TYPES) {
            const EntityTypeExpr* entityType = static_cast<const EntityTypeExpr*>(expr);
            pending.push_back(entityType->getEntityVarDecl());
        }
    }

    for (const EntityPattern* entity : _ast->getEntityPatterns()) {
        pending.push_back(entity->getDecl());
    }

    while (!pending.empty()) {
        const VarDecl* decl = pending.back();
        pending.pop_back();

        if (!decl || !read.insert(decl).second) {
            continue;
        }

        const auto sourceIt = _declSources.find(decl);
        if (sourceIt != _declSources.end()) {
            pending.push_back(sourceIt->second);
        }
    }
}
