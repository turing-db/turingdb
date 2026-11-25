#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include "EntityPattern.h"
#include "NodePattern.h"

#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "DBClauseItems.h"
#include "DBHiddenColumnNames.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"

#include "DependencyEdge.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "Pattern.h"
#include "PatternElement.h"
#include "WhereClause.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "expr/Expr.h"
#include "stmt/MatchStmt.h"

#include "BioAssert.h"

using namespace db;

void DBProgramGenerator::collectComponentRoots(llvm::SmallVectorImpl<const VariableDependency*>& roots) const {
    // Every variable a component holds can be a valid root of its own - the target of an
    // expansion has no incoming edge that produces an edge variable either - so a component
    // is opened by the first of them the graph lists, and the rest are reached from it.
    std::unordered_set<const VariableDependency*> visited;

    for (const VariableDependency& var : _vdg.vars()) {
        const bool opensComponent = !visited.contains(&var) && isValidRoot(var);
        if (!opensComponent) {
            continue;
        }

        roots.push_back(&var);

        llvm::SmallVector<const VariableDependency*> worklist {&var};
        while (!worklist.empty()) {
            const VariableDependency* const current = worklist.pop_back_val();
            if (!visited.insert(current).second) {
                continue;
            }

            for (const DependencyEdge* edge : current->edges()) {
                worklist.push_back(edge->src() == current ? edge->tgt() : edge->src());
            }
        }
    }
}

// Breaking a cycle leaves the variable it closed on standing behind one merge edge per
// end, and that variable carries no rows until every end it merges holds some. A root
// inside the broken cycle reaches those ends without crossing it; one outside reaches the
// variable first and stalls there, having nothing to open the ends with.
bool DBProgramGenerator::reachesTheMergesItCrosses(const VariableDependency& root) const {
    const auto isMerged = [](const VariableDependency* var) {
        return std::ranges::any_of(var->incoming(),
                                   [](const DependencyEdge* edge) { return edge->isMetaEdge(); });
    };

    llvm::SmallVector<const VariableDependency*> worklist {&root};
    std::unordered_set<const VariableDependency*> reached {&root};
    llvm::SmallVector<const VariableDependency*> crossed;

    while (!worklist.empty()) {
        const VariableDependency* const current = worklist.pop_back_val();

        if (isMerged(current)) {
            crossed.push_back(current);
            continue;
        }

        for (const DependencyEdge* edge : current->edges()) {
            const VariableDependency* const other = edge->src() == current ? edge->tgt() : edge->src();
            if (reached.insert(other).second) {
                worklist.push_back(other);
            }
        }
    }

    return std::ranges::all_of(crossed, [&reached](const VariableDependency* var) {
        return std::ranges::all_of(var->incoming(), [&reached](const DependencyEdge* edge) {
            return !edge->isMetaEdge() || reached.contains(edge->src());
        });
    });
}

void DBProgramGenerator::collectOrderedRoots(llvm::SmallVectorImpl<const VariableDependency*>& roots) const {
    for (const VariableDependency& var : _vdg.vars()) {
        if (isValidRoot(var)) {
            roots.push_back(&var);
        }
    }

    // Both ends a broken cycle leaves behind can open it, but only the one the pattern
    // traverses out of walks it the way the pattern was written; opening it at the other
    // emits the reverse traversal, whose path lists its hops back to front.
    const auto walksOutOfABrokenCycle = [](const VariableDependency* root) {
        const VariableDependency::Edges& outgoing = root->outgoing();

        const auto isMeta = [](const DependencyEdge* edge) { return edge->isMetaEdge(); };

        return std::ranges::any_of(outgoing, isMeta)
            && !std::ranges::all_of(outgoing, isMeta);
    };

    const auto opensItsOwnMerges = [this](const VariableDependency* root) {
        return reachesTheMergesItCrosses(*root);
    };

    std::stable_partition(roots.begin(), roots.end(), walksOutOfABrokenCycle);
    std::stable_partition(roots.begin(), roots.end(), opensItsOwnMerges);
}

void DBProgramGenerator::generateTraversal(std::span<Stmt* const> stmts) {
    if (_vdg.empty()) {
        return;
    }

    // Main block is saved so we can splice into it after generation
    mlir::Block* const mainBlock = _opBuilder.getInsertionBlock();

    DefinedVars defined;

    // The dataflow already standing in the main block: the component a leading call drives
    // from a column it bound there, or the one a barrier left behind, extended by the hops
    // of this part.
    TranslatedComponent mainComponent;

    const VariableDependencyGraph::BoundVars& bound = _vdg.boundVars();
    if (_part._drivenRoot) {
        // A column the barrier published holds its rows already, so it opens no component
        // of its own: it stands in the main block beside the rows the leading statements
        // bound, and rides with them into any island crossed with them.
        for (const VariableDependency* var : bound) {
            defined.insert(var);

            if (!yieldsConstantColumn(findVarOrThrow(_part._varMap, var), _constantColumns)) {
                mainComponent._vars.push_back(var);
            }
        }

        translateComponent(_part._drivenRoot, defined, mainComponent._vars);
    } else if (!bound.empty()) {
        extendBoundDataflow(defined, mainComponent._vars);
    }

    // Connected components
    std::vector<TranslatedComponent> components;

    llvm::SmallVector<const VariableDependency*> roots;
    collectOrderedRoots(roots);

    llvm::SmallVector<const VariableDependency*> crossedRoots;
    DefinedVars crossedComponentVars;

    // TODO: Use nodes at ends of diameter
    for (const VariableDependency* root : roots) {
        if (defined.contains(root) || crossedComponentVars.contains(root)) {
            continue;
        }

        TranslatedComponent& component = components.emplace_back();
        component._region = std::make_unique<mlir::Region>();

        mlir::Block* const scratch = new mlir::Block();
        component._region->push_back(scratch); // Region destructor frees scratch
        _opBuilder.setInsertionPointToStart(scratch);

        DefinedVars componentVars;
        collectComponentVars(root, componentVars);

        if (walkReadsAnotherComponent(componentVars)) {
            openComponent(root);
            defined.insert(root);
            component._vars.push_back(root);

            crossedRoots.push_back(root);
            crossedComponentVars.insert(componentVars.begin(), componentVars.end());
        } else {
            translateComponent(root, defined, component._vars);
        }

        for (const VariableDependency* var : component._vars) {
            component._columns.push_back(findVarOrThrow(_part._varMap, var));
        }
    }

    if (components.empty()) {
        return;
    }

    // A pattern naming none of the variables already flowing matches on its own, so its
    // rows are crossed with theirs. A scope of constants alone carries no rows to cross:
    // one value stands for every row of either factor
    if (!mainComponent._vars.empty()) {
        takeMainDataflow(mainBlock, mainComponent);
        components.insert(components.begin(), std::move(mainComponent));
    }

    // Single connected component, no need to X prod any islands
    if (components.size() == 1) {
        TranslatedComponent& comp = components.front();

        const auto& reg = comp._region;
        bioassert(reg->hasOneBlock(), "Connected component region did not have 1 block");

        // Get the ops for this connected component
        mlir::Block& block = reg->front();
        mlir::Block::OpListType& ops = block.getOperations();

        // Get the block for main
        const auto mainEnd = mainBlock->end();
        mlir::Block::OpListType& mainOps = mainBlock->getOperations();

        // Splice the ops for this component into the end of main
        mainOps.splice(mainEnd, ops);

        _opBuilder.setInsertionPointToEnd(mainBlock);
        return;
    }

    llvm::SmallVector<mlir::Value> results;
    buildCrossProductCascade(components, mainBlock, results);

    size_t resultIndex = 0;
    for (const TranslatedComponent& component : components) {
        for (const VariableDependency* var : component._vars) {
            registerValue(var, results[resultIndex]);
            resultIndex++;
        }
    }

    _opBuilder.setInsertionPointToEnd(mainBlock);

    expandCrossedComponents(crossedRoots, components, defined);
}

void DBProgramGenerator::resolveEdgeIdentities() {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType =
        allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    // Join Cypher edge variables which had multiple VariableDependency*s in @ref _vdg
    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        const bool needsFilter = vars.size() > 1;
        if (!needsFilter) {
            continue;
        }

        mlir::Value predicate;
        for (size_t index = 0; index + 1 < vars.size(); index++) {
            const VariableDependency* fstVar = vars[index];
            const VariableDependency* sndVar = vars[index + 1];
            const mlir::Value fstCol = findVarOrThrow(_part._varMap, fstVar);
            const mlir::Value sndCol = findVarOrThrow(_part._varMap, sndVar);

            auto eqOp = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstCol, sndCol);
            const mlir::Value eq = eqOp.getResult();

            if (!predicate) {
                predicate = eq;
            } else {
                auto andOp = _opBuilder.create<mlir::db::AndOp>(uloc, boolType, predicate, eq);
                predicate = andOp.getResult();
            }
        }

        filterAllColumns(predicate);
    }
}

void DBProgramGenerator::throwOnUnboundPatternVariable() const {
    for (const VariableDependency& var : _vdg.vars()) {
        if (holdsColumn(&var)) {
            continue;
        }

        throwError(fmt::format("Reaching the pattern variable '{}' from the rest "
                               "of the query is not yet supported.",
                               var.getName()),
                   var.getDecl());
    }
}

void DBProgramGenerator::throwOnDroppedUnwindSeed() const {
    for (const auto& [var, unwind] : _vdg.unwindSources()) {
        const VarDecl* const decl = var->getDecl();
        const bool namesAPatternNode = decl && decl->getType() == EvaluatedType::NodePattern;

        if (!namesAPatternNode || _part._seededVars.contains(var)) {
            continue;
        }

        throwError(fmt::format("Unwinding node IDs into the pattern variable '{}' is "
                               "not yet supported where the pattern reaches it from "
                               "elsewhere.",
                               var->getName()),
                   var->getDecl());
    }
}

void DBProgramGenerator::closeBoundMerges() {
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    for (const VariableDependency* var : _vdg.boundVars()) {
        for (const DependencyEdge* inEdge : var->incoming()) {
            if (!inEdge->isMetaEdge()) {
                continue;
            }

            const mlir::Value landed = findVarOrThrow(_part._varMap, inEdge->src());
            const mlir::Value bound = findVarOrThrow(_part._varMap, var);

            auto eq = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, landed, bound);
            filterAllColumns(eq.getResult());
        }
    }
}

void DBProgramGenerator::closeBoundEdges() {
    const VariableDependencyGraph::EdgeIdentityMap& identities = _vdg.edgeIdentities();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    for (const VariableDependency* var : _vdg.boundVars()) {
        const auto identityIt = identities.find(var->getDecl());
        if (identityIt == identities.end()) {
            continue;
        }

        const mlir::Value matched = findVarOrThrow(_part._varMap, identityIt->second.front());
        const mlir::Value bound = findVarOrThrow(_part._varMap, var);

        auto eq = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, matched, bound);
        filterAllColumns(eq.getResult());
    }
}

void DBProgramGenerator::resolveYieldedIdentities() {
    struct YieldedIdentity {
        const VariableDependency* _variable {nullptr};
        size_t _yieldedIndex {0};
    };

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();

    llvm::SmallVector<YieldedIdentity> identities;
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const VarDecl* yieldedDecl = _part._yieldedColumns[yieldedIndex]._decl;
        if (!yieldedDecl) {
            continue;
        }

        for (const auto& [var, values] : _part._varMap) {
            if (var->getDecl() != yieldedDecl) {
                continue;
            }

            identities.push_back({var, yieldedIndex});
        }

        const auto edgeIt = edgeIdentities.find(yieldedDecl);
        const bool yieldsAPatternEdge = edgeIt != edgeIdentities.end() && !edgeIt->second.empty();
        if (yieldsAPatternEdge) {
            identities.push_back({edgeIt->second.front(), yieldedIndex});
        }
    }

    if (identities.empty()) {
        return;
    }

    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    for (const YieldedIdentity& identity : identities) {
        const mlir::Value patternColumn = findVarOrThrow(_part._varMap, identity._variable);
        const mlir::Value yieldedColumn = _part._yieldedColumns[identity._yieldedIndex]._column;

        const bool patternAligned = isRowAlignedHere(patternColumn);
        const bool yieldedAligned = isRowAlignedHere(yieldedColumn);
        bioassert(patternAligned && yieldedAligned,
                  "Yielded variable {} was not paired with its pattern",
                  identity._variable->getName());

        auto eqOp = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, patternColumn, yieldedColumn);

        filterAllColumns(eqOp.getResult());
    }
}

void DBProgramGenerator::collectComponentVars(const VariableDependency* root, DefinedVars& componentVars) const {
    llvm::SmallVector<const VariableDependency*> worklist {root};
    while (!worklist.empty()) {
        const VariableDependency* const current = worklist.pop_back_val();
        if (!componentVars.insert(current).second) {
            continue;
        }

        for (const DependencyEdge* edge : current->edges()) {
            worklist.push_back(edge->src() == current ? edge->tgt() : edge->src());
        }
    }
}

void DBProgramGenerator::collectVarsOfDecl(const VarDecl* decl, llvm::SmallVectorImpl<const VariableDependency*>& vars) const {
    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto identityIt = edgeIdentities.find(decl);
    if (identityIt != edgeIdentities.end()) {
        vars.append(identityIt->second.begin(), identityIt->second.end());
    }

    for (const VariableDependency& var : _vdg.vars()) {
        if (var.getDecl() == decl) {
            vars.push_back(&var);
        }
    }
}

bool DBProgramGenerator::walkReadsAnotherComponent(const DefinedVars& componentVars) {
    const auto inComponent = [&componentVars](const VariableDependency* var) {
        return componentVars.contains(var);
    };

    llvm::SmallVector<const VariableDependency*> walkVars;
    llvm::SmallVector<const VariableDependency*> importVars;

    for (const auto& [edgeDecl, pattern] : _part._quantifiedEdges) {
        walkVars.clear();
        collectVarsOfDecl(edgeDecl, walkVars);

        if (!llvm::any_of(walkVars, inComponent)) {
            continue;
        }

        for (const VarDecl* import : pattern->hopImports()) {
            importVars.clear();
            collectVarsOfDecl(import, importVars);

            for (const VariableDependency* var : importVars) {
                if (inComponent(var)) {
                    continue;
                }

                const bool holdsRows = !holdsColumn(var) || !yieldsConstantColumn(findVarOrThrow(_part._varMap, var), _constantColumns);
                if (holdsRows) {
                    return true;
                }
            }
        }
    }

    return false;
}

void DBProgramGenerator::takeMainDataflow(mlir::Block* mainBlock, TranslatedComponent& component) {
    for (const VariableDependency* var : component._vars) {
        component._columns.push_back(findVarOrThrow(_part._varMap, var));
    }

    component._region = std::make_unique<mlir::Region>();
    mlir::Block* const scratch = new mlir::Block(); // Region destructor frees scratch
    component._region->push_back(scratch);

    const auto bindsAConstant = [this](mlir::Value result) {
        return yieldsConstantColumn(result, _constantColumns);
    };

    // A constant is left above the product: it holds one value standing for every row, so
    // neither factor crosses it and both read the one binding
    for (mlir::Operation& operation : llvm::make_early_inc_range(*mainBlock)) {
        const bool bindsColumns = operation.getNumResults() > 0;
        const bool bindsConstantsOnly = bindsColumns
                                        && llvm::all_of(operation.getResults(), bindsAConstant);

        if (bindsConstantsOnly) {
            continue;
        }

        operation.moveBefore(scratch, scratch->end());
    }
}

void DBProgramGenerator::buildCrossProductCascade(std::vector<TranslatedComponent>& components,
                                                  mlir::Block* targetBlock,
                                                  llvm::SmallVectorImpl<mlir::Value>& results) {
    const size_t numComponents = components.size();
    bioassert(numComponents >= 2, "Cross product cascade needs at least two components");

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    mlir::Block* currentTarget = targetBlock;

    mlir::Block* pendingYieldBlock = nullptr;

    // Fold over all components, applying a CrossProduct between them
    // comp1 x (comp2 x (comp3 x ...))
    for (size_t i = 0; i + 1 < numComponents; i++) {
        // Component i crossed with the cross of all subsequent component js
        // The result of this cross is result of i x i + 1 x i + 2 x ... x j
        llvm::SmallVector<mlir::Type> resultTypes;
        for (size_t j = i; j < numComponents; j++) {
            for (const mlir::Value column : components[j]._columns) {
                resultTypes.push_back(column.getType());
            }
        }

        // Starts as main block, then updated to the RHS of the previous factor
        _opBuilder.setInsertionPointToEnd(currentTarget);
        // Create a cross for i x j
        auto crossProduct = _opBuilder.create<mlir::db::CrossProduct>(loc, resultTypes);

        mlir::Block* const leftBlock = &crossProduct.getLeftFactor().front();
        mlir::Block* const rightBlock = &crossProduct.getRightFactor().front();

        // Component i occupies the LHS of this cross prod
        moveComponentToFactor(components[i], leftBlock);

        const mlir::ResultRange crossResults = crossProduct.getResults();
        // There is no pending yield iff we have a single cross product between 2
        // components
        if (pendingYieldBlock) {
            _opBuilder.setInsertionPointToEnd(pendingYieldBlock);
            _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {crossResults});
        } else {
            results.assign(crossResults.begin(), crossResults.end());
        }

        // If this is the final cross prod, move this component to the right hand side
        const bool lastPair = i + 2 == numComponents;
        if (lastPair) {
            moveComponentToFactor(components[i + 1], rightBlock);
        } else {
            // Otherwise the next component will be assigned to the right block
            currentTarget = rightBlock;
            pendingYieldBlock = rightBlock;
        }
    }
}

void DBProgramGenerator::moveComponentToFactor(TranslatedComponent& component,
                                               mlir::Block* factorBlock) {
    // Insertion point into the factor
    auto factorEnd = factorBlock->end();

    auto& compReg = component._region;
    mlir::Block& compBlock = compReg->front();
    // Operations to insert
    mlir::Block::OpListType& compOps = compBlock.getOperations();

    factorBlock->getOperations().splice(factorEnd, compOps);

    _opBuilder.setInsertionPointToEnd(factorBlock);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    _opBuilder.create<mlir::db::Yield>(loc, mlir::ValueRange {component._columns});
}

void DBProgramGenerator::generateMatchFilter(const MatchStmt* matchStmt) {
    const Pattern* pattern = matchStmt->getPattern();
    const WhereClause* where = pattern->getWhere();
    if (!where) {
        return;
    }

    std::vector<const Expr*> conjuncts;
    flattenConjuncts(where->getExpr(), conjuncts);

    applyPredicateFilters(conjuncts);
}

void DBProgramGenerator::generateOptionalMatch(std::span<Stmt* const> stmt) {
    const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt.front());

    throwOnMatchOverWrittenEntity(matchStmt, "An OPTIONAL MATCH");

    llvm::SmallVector<PublishedColumn> scopeColumns;
    collectPublishedColumns(scopeColumns);

    // A constant holds one value standing for every row rather than rows of its own, so it
    // is no row of this join: it stays bound where it is and the pattern reads it there
    llvm::SmallVector<PublishedColumn> inputs;
    llvm::SmallVector<PublishedColumn> constants;

    for (const PublishedColumn& column : scopeColumns) {
        if (yieldsConstantColumn(column._column, _constantColumns)) {
            constants.push_back(column);
        } else {
            inputs.push_back(column);
        }
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    mlir::Block* const outerBlock = _opBuilder.getInsertionBlock();

    // Built into a scratch region: the op's results are the columns the pattern turns out
    // to yield, which is known only once it is generated
    mlir::Region pattern;
    mlir::Block* const patternBlock = new mlir::Block(); // Region destructor frees it
    pattern.push_back(patternBlock);

    llvm::SmallVector<mlir::Value> inputColumns;
    llvm::SmallVector<PublishedColumn> patternScope;
    for (const PublishedColumn& input : inputs) {
        inputColumns.push_back(input._column);
        patternScope.push_back({input._decl, input._name, patternBlock->addArgument(input._column.getType(), loc)});
    }

    // No input column means no input row to tag: the rows the pattern joins onto are the
    // single empty row the query starts from, which any match marks
    const bool tagsRows = !inputs.empty();
    mlir::Value tagArgument;
    if (tagsRows) {
        const mlir::db::ColumnType tagType = allocColumnType(_opBuilder.getIntegerType(64, /*isSigned=*/false));
        tagArgument = patternBlock->addArgument(tagType, loc);
    }

    patternScope.append(constants.begin(), constants.end());

    rebindScopeKeepingWrittenEntities(patternScope);

    const VariableDependency* tagVariable = nullptr;
    if (tagsRows) {
        tagVariable = _vdg.registerBoundVariable(optionalTagName, nullptr);
        registerValue(tagVariable, tagArgument);
    }

    _opBuilder.setInsertionPointToStart(patternBlock);
    generatePart(stmt);

    llvm::SmallVector<PublishedColumn> matched;
    collectPublishedColumns(matched);

    const auto findMatched = [&matched](std::string_view name) {
        const auto foundIt = std::ranges::find(matched, name, &PublishedColumn::_name);
        bioassert(foundIt != matched.end(), "Column '{}' lost by an OPTIONAL MATCH pattern", name);
        return foundIt;
    };

    const auto boundOutside = [&inputs, &constants](std::string_view name) {
        const auto sameName = [name](const PublishedColumn& candidate) {
            return candidate._name == name;
        };

        return std::ranges::any_of(inputs, sameName) || std::ranges::any_of(constants, sameName);
    };

    // The columns the pattern contributes: the ones it joins onto first, in operand order,
    // then the variables of its own - what a row it missed comes back with null
    llvm::SmallVector<PublishedColumn> yielded;
    for (const PublishedColumn& input : inputs) {
        yielded.push_back(*findMatched(input._name));
    }

    // A handle into the path trie holds for the chunk that emitted it, and the accumulator
    // keeps rows across chunks, so a walk's column goes in as the list or path it stands for
    for (const PublishedColumn& column : matched) {
        if (column._name == optionalTagName || boundOutside(column._name)) {
            continue;
        }

        PublishedColumn padded = column;
        padded._column = listColumnOf(column._decl, column._column);
        yielded.push_back(padded);
    }

    llvm::SmallVector<mlir::Value> yieldedColumns;
    llvm::SmallVector<mlir::Type> resultTypes;
    for (const PublishedColumn& column : yielded) {
        yieldedColumns.push_back(column._column);
        resultTypes.push_back(column._column.getType());
    }

    const mlir::Value carriedTag = tagsRows ? findVarOrThrow(_part._varMap, tagVariable) : mlir::Value();
    _opBuilder.create<mlir::db::OptionalYield>(loc, carriedTag, yieldedColumns);

    _opBuilder.setInsertionPointToEnd(outerBlock);

    auto optionalMatch = _opBuilder.create<mlir::db::OptionalMatch>(loc, resultTypes, inputColumns);
    optionalMatch.getPattern().takeBody(pattern);

    llvm::SmallVector<PublishedColumn> published {yielded};
    for (size_t index = 0; index < published.size(); index++) {
        published[index]._column = optionalMatch.getResult(index);
    }

    published.append(constants.begin(), constants.end());

    rebindScopeKeepingWrittenEntities(published);

    // An OPTIONAL MATCH's cut reads the rows the join produced, the padded ones included
    generateMatchOrderBy(matchStmt);
    generateMatchWindow(matchStmt);
}

// An optional pattern reads the graph, which holds nothing this change wrote until the
// commit. A pattern naming such an entity is turned away; one that only carries it past the
// join is not, because the join hands its input columns back unchanged
void DBProgramGenerator::throwOnMatchOverWrittenEntity(const MatchStmt* matchStmt,
                                                       std::string_view clause) const {
    const Pattern* pattern = matchStmt->getPattern();

    for (const PatternElement* element : pattern->elements()) {
        for (const EntityPattern* entity : element->getEntities()) {
            const VarDecl* decl = entity->getDecl();

            if (isPendingThroughout(decl)) {
                throwError(fmt::format("{} cannot read what a CREATE in the same query wrote: '{}'",
                                       clause,
                                       decl->getName()),
                           matchStmt);
            } else if (findPendingMask(decl)) {
                throwError(fmt::format("{} cannot read what a MERGE in the same query wrote: '{}'",
                                       clause,
                                       decl->getName()),
                           matchStmt);
            }
        }
    }
}

void DBProgramGenerator::generateMatchConstraints(const MatchStmt* matchStmt) {
    const Pattern* pattern = matchStmt->getPattern();

    std::vector<const Expr*> constraintExprs;

    for (const PatternElement* element : pattern->elements()) {
        const NodePattern* rootNode = static_cast<const NodePattern*>(element->getRootEntity());
        const NodePatternData* rootData = rootNode->getData();

        if (rootData) {
            for (const EntityPropertyConstraint& constraint : rootData->exprConstraints()) {
                constraintExprs.push_back(constraint._expr);
            }
        }

        for (auto [edgePattern, nodePattern] : element->getElementChain()) {
            const EdgePatternData* edgeData = edgePattern->getData();
            if (edgeData) {
                for (const EntityPropertyConstraint& constraint : edgeData->exprConstraints()) {
                    constraintExprs.push_back(constraint._expr);
                }
            }

            const NodePatternData* nodeData = nodePattern->getData();
            if (nodeData) {
                for (const EntityPropertyConstraint& constraint : nodeData->exprConstraints()) {
                    constraintExprs.push_back(constraint._expr);
                }
            }
        }
    }

    applyPredicateFilters(constraintExprs);
}
