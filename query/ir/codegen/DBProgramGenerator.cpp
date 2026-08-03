#include "DBProgramGenerator.h"

#include <algorithm>
#include <memory>
#include <string_view>
#include <type_traits>
#include <variant>

#include "EntityPattern.h"
#include "NodePattern.h"
#include "expr/Operators.h"
#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "mlir/IR/Block.h"
#include "mlir/IR/Builders.h"
#include "mlir/IR/BuiltinOps.h"
#include "mlir/IR/Region.h"
#include "mlir/IR/TypeRange.h"
#include "mlir/IR/Value.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Interfaces/DataLayoutInterfaces.h"
#include "llvm/ADT/SmallVector.h"

#include "DBDialect.h"
#include "DBOps.h"
#include "DBTypes.h"
#include "StorageDialect.h"
#include "StorageTypes.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "CypherAST.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "Projection.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "WhereClause.h"
#include "decl/EvaluatedType.h"
#include "decl/PatternData.h"
#include "decl/VarDecl.h"
#include "FunctionInvocation.h"
#include "FunctionSignature.h"
#include "Literal.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/LiteralExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/StructuralExpressionComparator.h"
#include "expr/SymbolExpr.h"
#include "expr/UnaryExpr.h"
#include "stmt/CreateStmt.h"
#include "stmt/DeleteStmt.h"
#include "expr/ExprChain.h"
#include "stmt/Limit.h"
#include "stmt/MatchStmt.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ReturnStmt.h"
#include "stmt/SetItem.h"
#include "stmt/SetStmt.h"
#include "stmt/Skip.h"
#include "stmt/StmtContainer.h"
#include "Symbol.h"
#include "SymbolChain.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "TuringException.h"

using namespace db;

namespace {

bool producesEdgeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getOut = producedType == EdgeMetadata::EdgeType::GET_OUT_EDGES;
    const bool getIn = producedType == EdgeMetadata::EdgeType::GET_IN_EDGES;
    const bool getEdges = producedType == EdgeMetadata::EdgeType::GET_EDGES;
    return getOut || getIn || getEdges;
}

bool producesNodeVar(const DependencyEdge* e) {
    const EdgeMetadata::EdgeType producedType = e->data().type();
    const bool getTgt = producedType == EdgeMetadata::EdgeType::GET_EDGE_TGT;
    const bool getSrc = producedType == EdgeMetadata::EdgeType::GET_EDGE_SRC;
    return getTgt || getSrc;
}

EdgeMetadata::EdgeType reverseEdge(EdgeMetadata::EdgeType type) {
    switch (type) {
        case EdgeMetadata::EdgeType::GET_OUT_EDGES:
            return EdgeMetadata::EdgeType::GET_IN_EDGES;
        break;

        case EdgeMetadata::EdgeType::GET_IN_EDGES:
            return db::EdgeMetadata::EdgeType::GET_OUT_EDGES;
        break;

        case EdgeMetadata::EdgeType::GET_EDGES:
            return db::EdgeMetadata::EdgeType::GET_EDGES;
        break;

        default:
            throw FatalException("Invalid attempt to reverse direction");
        break;
    }

    throw FatalException("Uncaught edge type.");

}

// The index of the projected column @param key reads, or the item count when the
// projection does not carry it. The projection is one column per return item, so the
// index is the column's index too.
//
// A key and the item it reads are separate AST nodes - the n.name of
// RETURN n.name ORDER BY n.name is written twice, so it is parsed twice - which is why
// the two are matched by structure rather than by identity
size_t findProjectedColumn(const Projection::Items& items, const Expr* key) {
    const VarDecl* keyDecl = key->getExprVarDecl();

    size_t index = 0;
    for (const Projection::ReturnItem& item : items) {
        if (const Expr* const* itemExpr = std::get_if<Expr*>(&item)) {
            const Expr* projectedExpr = *itemExpr;

            // A key may also name the alias an item was given - the x of
            // RETURN n.name AS x ORDER BY x - which is one variable declared once, so the
            // key and the item share its declaration. Every other expression is given a
            // declaration of its own, so only an alias is ever matched here
            const bool namesTheItem = keyDecl && projectedExpr->getExprVarDecl() == keyDecl;
            const bool readsTheItem = StructuralExpressionComparator::equal(projectedExpr, key);

            if (namesTheItem || readsTheItem) {
                return index;
            }
        }

        index++;
    }

    return index;
}

mlir::Value findVarOrThrow(const DBProgramGenerator::VariableIdentityMap& map,
                        const VariableDependency* var) {
    const auto findIt = map.find(var);
    bioassert(findIt != end(map), "Missing value for {}.", var->getName());
    const DBProgramGenerator::VariableIdentities& identities = findIt->second;
    bioassert(!identities.empty(), "Missing identity for {}.", var->getName());
    return identities.back();
}

}

DBProgramGenerator::DBProgramGenerator(mlir::ModuleOp* mainModule)
    : _module(mainModule),
    _mlirCtxt(_module->getContext()),
    _opBuilder(_module->getBodyRegion())
{
}

DBProgramGenerator::~DBProgramGenerator() {
}

mlir::db::ColumnType DBProgramGenerator::allocColumnType(mlir::Type type) {
    return mlir::db::ColumnType::get(_mlirCtxt, type);
}

void DBProgramGenerator::registerValue(const VariableDependency* var, mlir::TypedValue<mlir::Type> val) {
    _varMap[var].emplace_back(val);
}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_varMap.contains(var), "ScanNodes for registered variable");

    const auto col = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    auto scan = _opBuilder.create<mlir::db::ScanNodes>(_opBuilder.getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

template<typename EdgeOp>
void DBProgramGenerator::addEdgeTraversal(const VariableDependency* src,
                                          const VariableDependency* edge,
                                          const VariableDependency* tgt,
                                          const std::vector<const VariableDependency*>& carrySet) {
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetEdges>, "Invalid op");

    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    bioassert(_varMap.contains(src), "Edge traversal without source");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto eids = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const auto etypes = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    const mlir::Value input = _varMap[src].back();

    llvm::SmallVector<const VariableDependency*> carried;
    llvm::SmallVector<mlir::Value> operands {input};
    llvm::SmallVector<mlir::Type> results {srcs, eids, etypes, tgts};
    for (const VariableDependency* var : carrySet) {
        // source variable is explicitly filtered by the edge op
        if (var == src) {
            continue;
        }

        const mlir::Value column = _varMap[var].back();
        carried.push_back(var);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    // Find the edge types to carry which were defined in this block
    mlir::Block* const insertionBlock = _opBuilder.getInsertionBlock();
    llvm::SmallVector<const VariableDependency*> carriedEdgeTypes;
    for (auto& [edgeVar, column] : _edgeTypeMap) {
        mlir::Operation* const definingOp = column.getDefiningOp();
        mlir::Block* const definingBlock = definingOp
            ? definingOp->getBlock()
            : mlir::cast<mlir::BlockArgument>(column).getOwner();
        if (definingBlock != insertionBlock) {
            continue;
        }
        carriedEdgeTypes.push_back(edgeVar);
        operands.push_back(column);
        results.push_back(column.getType());
    }

    const auto loc = _opBuilder.getUnknownLoc();
    auto op = _opBuilder.create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    const mlir::Value newEtypes = op.getResult(2);
    const mlir::Value newTgts = op.getResult(3);

    _edgeTypeMap[edge] = newEtypes;

    constexpr bool forwardOrientation = std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                                        or std::is_same_v<EdgeOp, mlir::db::GetEdges>;
    if constexpr (forwardOrientation) {
        registerValue(src, newSrcs);
        registerValue(tgt, newTgts);
    } else {
        registerValue(src, newTgts);
        registerValue(tgt, newSrcs);
    }
    registerValue(edge, newEdges);

    // Register the new values of the carry set, appearing starting from index 4 in the
    // result range
    constexpr size_t GET_X_EDGES_RES_SIZE = 4;
    for (size_t i = 0; i < carried.size(); i++) {
        const size_t resultIndex = GET_X_EDGES_RES_SIZE + i;
        registerValue(carried[i], op.getResult(resultIndex));
    }

    // Update the new edge type vars for carried edges
    const size_t edgeTypeOffset = GET_X_EDGES_RES_SIZE + carried.size();
    for (size_t i = 0; i < carriedEdgeTypes.size(); i++) {
        _edgeTypeMap[carriedEdgeTypes[i]] = op.getResult(edgeTypeOffset + i);
    }
}

void DBProgramGenerator::generate(const CypherAST* ast) {
    bioassert(_module, "Null module");
    bioassert(_mlirCtxt, "Null context");

    _mlirCtxt->loadDialect<mlir::db::DB>();
    _mlirCtxt->loadDialect<mlir::storage::Storage>();
    _mlirCtxt->loadDialect<mlir::func::FuncDialect>();
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    { // Create main
        _opBuilder.setInsertionPointToEnd(_module->getBody());
        const mlir::FunctionType funcType = mlir::FunctionType::get(_mlirCtxt, {}, {});
        auto func = _opBuilder.create<mlir::func::FuncOp>(uloc, "main", funcType);
        mlir::Block& block = *func.addEntryBlock();
        _opBuilder.setInsertionPointToStart(&block);
    }

    generateTraversal(ast);
    resolveEdgeIdentities();
    generatePropertyConstraints(ast);
    generateLabelConstraints(ast);
    generateEdgeTypeConstraints(ast);
    generateFilters(ast);
    generateGroupAggregate(ast);
    generateCreate(ast);
    generateSet(ast);
    generateDelete(ast);
    generateOutput(ast);

    _opBuilder.create<mlir::func::ReturnOp>(uloc);
}

void DBProgramGenerator::generateTraversal(const CypherAST* ast) {
    _vdg.buildFromAST(ast);

    if (_vdg.empty()) {
        return;
    }

    // Main block is saved so we can splice into it after generation
    mlir::Block* const mainBlock = _opBuilder.getInsertionBlock();

    DefinedVars defined;

    // Connected components
    std::vector<TranslatedComponent> components;

    // TODO: Use nodes at ends of diameter
    for (const VariableDependency& root : _vdg.vars()) {
        if (defined.contains(&root)) {
            continue;
        }

        const auto isEdgeTgtMetaVar = [](const DependencyEdge* e) -> bool {
            return e->isMetaEdge();
        };

        // A valid root is a non-meta Cypher variable which is a node
        const bool validRoot =
            std::ranges::none_of(root.incoming(), [&](const DependencyEdge* e) {
                return producesEdgeVar(e) || isEdgeTgtMetaVar(e);
            });

        if (!validRoot) {
            continue;
        }

        TranslatedComponent& component = components.emplace_back();
        component._region = std::make_unique<mlir::Region>();

        mlir::Block* const scratch = new mlir::Block();
        component._region->push_back(scratch); // Region destructor frees scratch
        _opBuilder.setInsertionPointToStart(scratch);

        translateComponent(&root, defined, component._vars);

        for (const VariableDependency* var : component._vars) {
            bioassert(_varMap.contains(var), "Component var {} not registered", var->getName());
            component._columns.push_back(_varMap[var].back());
        }
    }

    if (components.empty()) {
        return;
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
}

void DBProgramGenerator::filterAllColumns(mlir::Value predicate) {
    if (_varMap.empty()) {
        return;
    }

    // Only filter columns defined in the current insertion block
    mlir::Block* const insertionBlock = _opBuilder.getInsertionBlock();

    llvm::SmallVector<mlir::Value> columnsToFilter;
    llvm::SmallVector<const VariableDependency*> orderedVars;
    for (auto& [var, values] : _varMap) {
        const mlir::Value column = values.back();
        mlir::Operation* const definingOp = column.getDefiningOp();
        mlir::Block* const definingBlock = definingOp
            ? definingOp->getBlock()
            : mlir::cast<mlir::BlockArgument>(column).getOwner();
        if (definingBlock != insertionBlock) {
            continue;
        }
        columnsToFilter.push_back(column);
        orderedVars.push_back(var);
    }

    llvm::SmallVector<const VariableDependency*> orderedEdgeTypeVars;
    for (auto& [var, column] : _edgeTypeMap) {
        mlir::Operation* const definingOp = column.getDefiningOp();
        mlir::Block* const definingBlock = definingOp
            ? definingOp->getBlock()
            : mlir::cast<mlir::BlockArgument>(column).getOwner();
        if (definingBlock != insertionBlock) {
            continue;
        }
        columnsToFilter.push_back(column);
        orderedEdgeTypeVars.push_back(var);
    }

    if (columnsToFilter.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value column : columnsToFilter) {
        resultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto filterOp = _opBuilder.create<mlir::db::FilterOp>(loc, resultTypes, predicate, columnsToFilter);

    for (size_t index = 0; index < orderedVars.size(); index++) {
        registerValue(orderedVars[index], filterOp.getResult(index));
    }

    const size_t edgeTypeOffset = orderedVars.size();
    for (size_t index = 0; index < orderedEdgeTypeVars.size(); index++) {
        _edgeTypeMap[orderedEdgeTypeVars[index]] = filterOp.getResult(edgeTypeOffset + index);
    }
}

void DBProgramGenerator::addMergeFilter(const VariableDependency* mergeVar,
                                        std::vector<const VariableDependency*>& carriedSet) {
    const VariableDependency* fstMergeSource = nullptr;
    const VariableDependency* sndMergeSource = nullptr;
    for (const DependencyEdge* inEdge : mergeVar->incoming()) {
        if (!inEdge->isMetaEdge()) {
            continue;
        }
        if (!fstMergeSource) {
            fstMergeSource = inEdge->src();
        } else {
            sndMergeSource = inEdge->src();
        }
    }
    bioassert(fstMergeSource && sndMergeSource, "MERGE target without two sources");

    const mlir::Location uloc = _opBuilder.getUnknownLoc();
    const mlir::Value fstSourceCol = _varMap.at(fstMergeSource).back();
    const mlir::Value sndSourceCol = _varMap.at(sndMergeSource).back();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    // Create an EQ op to keep only rows where both sources are the same
    auto eq =
        _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstSourceCol, sndSourceCol);
    const mlir::Value eqRes = eq.getResult();

    // Register mergeVar's initial value (first source, arbitrary) so filterAllColumns
    // picks it up along with the rest of _varMap.
    registerValue(mergeVar, fstSourceCol);
    filterAllColumns(eqRes);
    carriedSet.push_back(mergeVar);
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
            const mlir::Value fstCol = findVarOrThrow(_varMap, fstVar);
            const mlir::Value sndCol = findVarOrThrow(_varMap, sndVar);

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

void DBProgramGenerator::translateComponent(const VariableDependency* root,
                                            DefinedVars& defined,
                                            std::vector<const VariableDependency*>& outVars) {
    struct Frame {
        const VariableDependency* _var {nullptr};
        const DependencyEdge* _predEdge {nullptr};
        const DependencyEdge* _predPredEdge {nullptr};
    };

    // Makes var an output on first encounter
    const auto markDefined = [&](const VariableDependency* var) {
        if (defined.insert(var).second) {
            outVars.push_back(var);
        }
    };

    addScanNodes(root);
    markDefined(root);

    // Forms the "carried set" for this connected component
    std::vector<const VariableDependency*> carriedSet;

    std::vector<Frame> stack;

    // DFS from this root
    stack.emplace_back(root, nullptr);
    while (!stack.empty()) {
        const auto [var, pred, predPred] = stack.back();
        stack.pop_back();

        const auto seenOrMeta = [&defined](const DependencyEdge* e) {
            return !e->isMetaEdge() || defined.contains(e->src());
        };
        const bool canTraverse = std::ranges::all_of(var->incoming(), seenOrMeta);

        // If we cannot traverse now, we will find another path to this node
        if (!canTraverse) {
            continue;
        }

        // Have we found a (source, edge, target) triple yet on this traversal?
        const bool haveTriple = pred && predPred;

        for (const DependencyEdge* e : var->edges()) {
            const VariableDependency* other = e->src() == var ? e->tgt() : e->src();
            if (defined.contains(other)) {
                continue;
            }

            if (haveTriple) {
                // We have discovered a full (src, edge, tgt) triple, set the next
                // elements on the stack to only have (src, edge) and await tgt
                stack.emplace_back(other, e, nullptr);
            } else {
                // We have not yet discovered a full (src, edge, tgt) triple, but the
                // next elements on stack will have such a triple (with @ref pred)
                stack.emplace_back(other, e, pred);
            }
        }

        // Only translate when we have a full triple
        if (!haveTriple) {
            if (pred && pred->isMetaEdge()) {
                addMergeFilter(var, carriedSet);
            }

            markDefined(var);
            continue;
        }

        // The order we encountered the nodes may not be source, edge, target, it may
        // be target, edge, source. Determine a definitive order, irrespective of
        // traversal
        const DependencyEdge* edgeVarProd = producesEdgeVar(pred) ? pred : predPred;
        const DependencyEdge* nodeVarProd = producesNodeVar(pred) ? pred : predPred;
        bioassert(producesEdgeVar(edgeVarProd), "No edge producer");
        bioassert(producesNodeVar(nodeVarProd), "No node producer");

        // Only one of either source or target should be defined. Determine which end
        // of the triple is defined
        const bool edgeSrcDefined = defined.contains(edgeVarProd->src());
        const bool nodeTgtDefined = defined.contains(nodeVarProd->tgt());
        bioassert(edgeSrcDefined ^ nodeTgtDefined, "Ambiguous definition");
        bioassert(edgeSrcDefined || nodeTgtDefined, "No defined start");

        VariableDependency* src = nullptr;
        VariableDependency* edge = nullptr;
        VariableDependency* tgt = nullptr;

        // Orientate the operation such that the source operand is defined
        if (edgeSrcDefined) {
            src = edgeVarProd->src();
            edge = edgeVarProd->tgt();
            tgt = nodeVarProd->tgt();
        } else /* (nodeTgtDefined) */ {
            src = nodeVarProd->tgt();
            edge = nodeVarProd->src();
            tgt = edgeVarProd->src();
        }

        // We may walk an edge backwards compared to the cypher pattern. In such a
        // case we emit the opposite traversal.
        const EdgeMetadata::EdgeType prodType = edgeVarProd->data().type();
        const EdgeMetadata::EdgeType logicalDir =
            edgeSrcDefined ? prodType : reverseEdge(prodType);

        switch (logicalDir) {
            case EdgeMetadata::EdgeType::GET_OUT_EDGES:
                addGetOutEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_IN_EDGES:
                addGetInEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::MERGE:
                throw TuringException("MERGE edges not yet supported.");
            break;

            case EdgeMetadata::EdgeType::GET_EDGES:
                addGetEdges(src, edge, tgt, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_EDGE_TGT:
            case EdgeMetadata::EdgeType::GET_EDGE_SRC:
                throw FatalException(fmt::format("Attempted to translate {}",
                                                 EdgeTypeName::value(logicalDir)));
            break;

            case EdgeMetadata::EdgeType::_SIZE:
                throw FatalException("Attempted to translate invalid edge.");
            break;
        }

        markDefined(src);
        markDefined(edge);
        markDefined(tgt);

        carriedSet.push_back(src);
        carriedSet.push_back(edge);
        carriedSet.push_back(tgt);
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

void DBProgramGenerator::generateCreate(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    // Collect MATCH-bound columns by variable name so CREATE patterns can reference them.
    std::unordered_map<std::string_view, mlir::Value> knownVars;
    for (const auto& [var, identities] : _varMap) {
        if (!identities.empty()) {
            knownVars[var->getName()] = identities.back();
        }
    }

    const mlir::db::ColumnType nodeIDType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const mlir::db::ColumnType edgeIDType = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // Emit db.create_node for a node that is not already in knownVars, then register
    // the result under the node's variable name so later patterns can reference it.
    const auto resolveOrCreateNode = [&](const NodePattern* node) -> mlir::Value {
        const VarDecl* decl = node->getDecl();
        const std::string_view name = decl ? decl->getName() : "";

        if (!name.empty() && knownVars.contains(name)) {
            return knownVars.at(name);
        }

        llvm::SmallVector<llvm::StringRef> labelNames;
        const SymbolChain* labels = node->labels();
        if (labels) {
            for (const Symbol* sym : *labels) {
                const std::string_view symName = sym->getName();
                labelNames.push_back(llvm::StringRef(symName.data(), symName.size()));
            }
        }

        llvm::SmallVector<llvm::StringRef> propNames;
        llvm::SmallVector<mlir::Value> propValues;
        const NodePatternData* data = node->getData();
        if (data) {
            for (const EntityPropertyConstraint& constraint : data->exprConstraints()) {
                translateExpr(constraint._expr);
                const std::string_view propName = constraint._propTypeName;
                propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
                propValues.push_back(_exprMap.at(constraint._expr));
            }
        }

        mlir::db::CreateNode createNode = _opBuilder.create<mlir::db::CreateNode>(
            loc,
            nodeIDType,
            _opBuilder.getStrArrayAttr(labelNames),
            _opBuilder.getStrArrayAttr(propNames),
            mlir::ValueRange{propValues});
        const mlir::Value nodeValue = createNode.getResult();

        if (!name.empty()) {
            knownVars[name] = nodeValue;
        }

        return nodeValue;
    };

    llvm::SmallVector<llvm::StringRef> propNames;
    llvm::SmallVector<mlir::Value> propValues;

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::CREATE) {
            continue;
        }
        const CreateStmt* createStmt = static_cast<const CreateStmt*>(stmt);
        const Pattern* pattern = createStmt->getPattern();

        for (const PatternElement* element : pattern->elements()) {
            const EntityPattern* entPtn = element->getRootEntity();
            const auto* nodePtn = dynamic_cast<const NodePattern*>(entPtn);
            bioassert(nodePtn, "Unknown root entity");

            mlir::Value lhsValue = resolveOrCreateNode(nodePtn);

            for (auto [edge, targetNode] : element->getElementChain()) {
                const mlir::Value rhsValue = resolveOrCreateNode(targetNode);

                propNames.clear();
                propValues.clear();
                const EdgePatternData* edgeData = edge->getData();
                if (edgeData) {
                    for (const EntityPropertyConstraint& constraint : edgeData->exprConstraints()) {
                        translateExpr(constraint._expr);
                        const std::string_view propName = constraint._propTypeName;
                        propNames.push_back(llvm::StringRef(propName.data(), propName.size()));
                        propValues.push_back(_exprMap.at(constraint._expr));
                    }
                }

                bioassert(edgeData && !edgeData->edgeTypeConstraints().empty(),
                          "CREATE edge must have an edge type");
                const std::string_view edgeType = edgeData->edgeTypeConstraints().front();

                const mlir::Value srcValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? rhsValue : lhsValue;
                const mlir::Value tgtValue =
                    edge->getDirection() == EdgePattern::Direction::Backward ? lhsValue : rhsValue;

                _opBuilder.create<mlir::db::CreateEdge>(
                    loc,
                    edgeIDType,
                    srcValue,
                    tgtValue,
                    _opBuilder.getStringAttr(edgeType),
                    _opBuilder.getStrArrayAttr(propNames),
                    mlir::ValueRange{propValues});

                lhsValue = rhsValue;
            }
        }
    }
}

mlir::Value DBProgramGenerator::resolveEntityColumn(std::string_view varName) {
    for (const auto& [var, values] : _varMap) {
        if (var->getName() == varName && !values.empty()) {
            return values.back();
        }
    }

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();
    const auto findIt = edgeIdentities.find(std::string(varName));
    const bool foundEdgeIdentity = findIt != edgeIdentities.end() && !findIt->second.empty();
    if (foundEdgeIdentity) {
        const VariableDependency* representative = findIt->second.front();
        return _varMap.at(representative).back();
    }

    return mlir::Value {};
}

void DBProgramGenerator::generateSet(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::SET) {
            continue;
        }

        const SetStmt* setStmt = static_cast<const SetStmt*>(stmt);

        for (const SetItem* item : setStmt->getItems()) {
            const SetItem::PropertyExprAssign* assign =
                std::get_if<SetItem::PropertyExprAssign>(&item->item());
            bioassert(assign, "Only property-assignment SET items are supported");

            const PropertyExpr* propertyExpr = assign->_propTypeExpr;
            const VarDecl* entityDecl = propertyExpr->getEntityVarDecl();
            const std::string_view varName = entityDecl->getName();
            const std::string_view propName = propertyExpr->getPropName();

            const mlir::Value entityColumn = resolveEntityColumn(varName);
            bioassert(entityColumn, "SET on unknown variable: {}", varName);

            translateExpr(assign->_propValueExpr);
            const mlir::Value valueColumn = _exprMap.at(assign->_propValueExpr);

            const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);
            const EvaluatedType entityType = entityDecl->getType();
            const bool isNode = entityType == EvaluatedType::NodePattern;
            const bool isEdge = entityType == EvaluatedType::EdgePattern;
            bioassert(isNode || isEdge, "SET on non-entity variable: {}", varName);

            if (isNode) {
                _opBuilder.create<mlir::db::SetNodeProperty>(loc, entityColumn, propAttr, valueColumn);
            } else /* (isEdge) */ {
                _opBuilder.create<mlir::db::SetEdgeProperty>(loc, entityColumn, propAttr, valueColumn);
            }
        }
    }
}

void DBProgramGenerator::generateDelete(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const StmtContainer* updateStmts = sglPart->getUpdateStmts();
    if (!updateStmts) {
        return;
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Stmt* stmt : updateStmts->stmts()) {
        if (stmt->getKind() != Stmt::Kind::DELETE) {
            continue;
        }

        const DeleteStmt* deleteStmt = static_cast<const DeleteStmt*>(stmt);
        const bool detach = deleteStmt->isDetaching();

        for (const Expr* expr : *deleteStmt->getExpressions()) {
            if (expr->getKind() != Expr::Kind::SYMBOL) {
                throw TuringException("Expressions in DELETE statements can only be symbols");
            }

            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const VarDecl* decl = symbolExpr->getDecl();
            bioassert(decl, "DELETE target symbol has no declaration");
            const std::string_view varName = decl->getName();

            const mlir::Value entityColumn = resolveEntityColumn(varName);
            if (!entityColumn) {
                throw TuringException("Cannot delete unbound variable: " + std::string(varName));
            }

            const EvaluatedType entityType = decl->getType();
            const bool isNode = entityType == EvaluatedType::NodePattern;
            const bool isEdge = entityType == EvaluatedType::EdgePattern;

            if (isNode) {
                _opBuilder.create<mlir::db::DeleteNode>(loc, entityColumn, detach);
            } else if (isEdge) {
                _opBuilder.create<mlir::db::DeleteEdge>(loc, entityColumn);
            } else {
                throw TuringException("Can only delete nodes or edges");
            }
        }
    }
}

void DBProgramGenerator::generateOutput(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    };

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        throw TuringException("Non-single part queries are not yet supported.");
    }

    const ReturnStmt* rtn = sglPart->getReturnStmt();
    if (!rtn) {
        return;
    }

    const Projection* proj = rtn->getProjection();

    const Projection::Items& returned = proj->items();

    VariableColumnMap variableColumns;
    for (auto& [cypherVar, mlirCol] : _varMap) {
        const std::string_view varName = cypherVar->getName();

        bioassert(not mlirCol.empty(), "No definitions for {}", varName);
        const mlir::Value finalValue = mlirCol.back();

        variableColumns[varName] = finalValue;
    }

    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        bioassert(!vars.empty(), "Empty edge identity for '{}'", name);
        const VariableDependency* representative = vars.front();
        bioassert(_varMap.contains(representative), "Edge identity representative not in varMap");
        variableColumns[name] = _varMap.at(representative).back();
    }

    const auto getVarForItem = [&](auto&& item) -> mlir::Value {
        using Type = std::remove_cvref_t<decltype(item)>;

        if constexpr (std::is_same_v<Type, VarDecl*>) {
            const std::string_view name = item->getName();
            const auto findIt = variableColumns.find(name);
            bioassert(findIt != end(variableColumns), "Return variable '{}' not found", name);
            return findIt->second;
        } else {
            return getOrTranslateExprColumn(variableColumns, item);
        }
    };

    llvm::SmallVector<mlir::Value> outputted;
    for (const Projection::ReturnItem item : returned) {
        const mlir::Value itemCol = std::visit(getVarForItem, item);
        outputted.push_back(itemCol);
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();

    // DISTINCT dedups the projection, and everything after it works on the rows that
    // survive: the sort orders the distinct rows, and SKIP and LIMIT cut them
    if (proj->isDistinct()) {
        llvm::SmallVector<mlir::Type> distinctResultTypes;
        for (const mlir::Value column : outputted) {
            distinctResultTypes.push_back(column.getType());
        }

        auto distinctOp = _opBuilder.create<mlir::db::RemoveDuplicates>(loc, distinctResultTypes, mlir::ValueRange{outputted});
        outputted.assign(distinctOp.getResults().begin(), distinctOp.getResults().end());
    }

    // ORDER BY reorders the whole projection, so it comes before them too: SKIP and
    // LIMIT cut the sorted rows
    if (proj->hasOrderBy()) {
        translateOrderBy(proj, variableColumns, outputted);
    }

    if (proj->hasSkip()) {
        const Expr* skipExpr = proj->getSkip()->getExpr();
        const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(skipExpr);
        const IntegerLiteral* intLit = static_cast<const IntegerLiteral*>(litExpr->getLiteral());
        const uint64_t skipCount = static_cast<uint64_t>(intLit->getValue());

        llvm::SmallVector<mlir::Type> skipResultTypes;
        for (const mlir::Value column : outputted) {
            skipResultTypes.push_back(column.getType());
        }

        auto skipOp = _opBuilder.create<mlir::db::Skip>(loc, skipResultTypes, mlir::ValueRange{outputted}, skipCount);
        outputted.assign(skipOp.getResults().begin(), skipOp.getResults().end());
    }

    if (proj->hasLimit()) {
        const Expr* limitExpr = proj->getLimit()->getExpr();
        const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(limitExpr);
        const IntegerLiteral* intLit = static_cast<const IntegerLiteral*>(litExpr->getLiteral());
        const uint64_t limitCount = static_cast<uint64_t>(intLit->getValue());

        llvm::SmallVector<mlir::Type> limitResultTypes;
        for (const mlir::Value column : outputted) {
            limitResultTypes.push_back(column.getType());
        }

        auto limitOp = _opBuilder.create<mlir::db::Limit>(loc, limitResultTypes, mlir::ValueRange{outputted}, limitCount);
        outputted.assign(limitOp.getResults().begin(), limitOp.getResults().end());
    }

    _opBuilder.create<mlir::db::Output>(loc, mlir::ValueRange{outputted});
}

void DBProgramGenerator::translateOrderBy(const Projection* projection,
                                          const VariableColumnMap& variableColumns,
                                          llvm::SmallVectorImpl<mlir::Value>& projected) {
    const OrderBy* orderBy = projection->getOrderBy();
    const OrderBy::ItemVector& items = orderBy->getItems();
    bioassert(!items.empty(), "ORDER BY without a key");

    const Projection::Items& projectedItems = projection->items();
    const bool distinct = projection->isDistinct();

    // A sort reorders the rows of every column it is given at once, so it is given the
    // whole projection and the projected columns stay row-aligned. A key the projection
    // does not carry - the a.age of RETURN a ORDER BY a.age - is handed over as one more
    // column, so that it moves with the row it belongs to; its result is then left
    // unread, which is what keeps it out of the output
    llvm::SmallVector<mlir::Value> sorted {projected.begin(), projected.end()};

    llvm::SmallVector<int64_t> keyColumns;
    llvm::SmallVector<bool> keyAscending;

    // Keys are given most significant first, the order the Sort expects
    for (const OrderByItem* item : items) {
        const Expr* keyExpr = item->getExpr();

        // A constant key holds the same value in every row, so it changes no order:
        // ORDER BY 1, n.name orders by n.name alone
        if (!keyExpr->isDynamic()) {
            continue;
        }

        const size_t projectedIndex = findProjectedColumn(projectedItems, keyExpr);
        const bool isProjected = projectedIndex < projectedItems.size();

        // A key names the column to sort by through its position in the columns handed to
        // the sort: the position the projection already gives it, or the end of the set
        // when the key has to be appended - so two appended keys never share a position
        if (isProjected) {
            keyColumns.push_back(static_cast<int64_t>(projectedIndex));
        } else {
            // After a DISTINCT only the returned columns exist, which is why Cypher
            // rejects the query. An unprojected key was read before the dedup, so it
            // holds one row per pre-dedup row and cannot be sorted with the survivors
            if (distinct) {
                throw TuringException("ORDER BY with DISTINCT may only order by returned columns.");
            }

            sorted.push_back(getOrTranslateExprColumn(variableColumns, keyExpr));
            keyColumns.push_back(static_cast<int64_t>(sorted.size() - 1));
        }

        keyAscending.push_back(item->getType() == OrderByType::ASC);
    }

    // Every key was constant, so a sort would hand back the rows it was given in the
    // order it was given them: the projection is already what the ORDER BY asks for
    if (keyColumns.empty()) {
        return;
    }

    llvm::SmallVector<mlir::Type> sortResultTypes;
    for (const mlir::Value column : sorted) {
        sortResultTypes.push_back(column.getType());
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    auto sortOp = _opBuilder.create<mlir::db::Sort>(loc,
                                                    sortResultTypes,
                                                    mlir::ValueRange {sorted},
                                                    keyColumns,
                                                    keyAscending);

    // The columns pass through the sort in place, so each projected column becomes its
    // own result; the extra key columns end the result range and are left unread
    const mlir::ResultRange results = sortOp.getResults();
    for (size_t index = 0; index < projected.size(); index++) {
        projected[index] = results[index];
    }
}

mlir::Value DBProgramGenerator::getOrTranslateExprColumn(const VariableColumnMap& variableColumns,
                                                         const Expr* expr) {
    // One column is held per variable, under the name of that variable, so an expression
    // only has a column to be found there when it is a variable and nothing else.
    // Anything more is a computation over columns, and has to be translated
    if (expr->getKind() == Expr::Kind::SYMBOL) {
        const VarDecl* var = expr->getExprVarDecl();
        bioassert(var, "Symbol expression without a declaration.");

        const std::string_view name = var->getName();
        const auto findIt = variableColumns.find(name);

        // A variable this holds no column for is not resolved here: translateExpr looks
        // it up among the variables the query traverses, and reports it as unknown if it
        // is not one of them either
        if (findIt != end(variableColumns)) {
            return findIt->second;
        }
    }

    translateExpr(expr);
    return _exprMap.at(expr);
}

void DBProgramGenerator::generatePropertyConstraints(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    std::vector<const Expr*> constraintExprs;

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();

        constraintExprs.clear();

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

        if (constraintExprs.empty()) {
            continue;
        }

        const mlir::Location loc = _opBuilder.getUnknownLoc();
        const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

        mlir::Value combinedPredicate;

        for (const Expr* constraintExpr : constraintExprs) {
            translateExpr(constraintExpr);
            const mlir::Value predicate = _exprMap.at(constraintExpr);

            if (!combinedPredicate) {
                combinedPredicate = predicate;
            } else {
                combinedPredicate = _opBuilder
                                        .create<mlir::db::AndOp>(
                                            loc, boolType, combinedPredicate, predicate)
                                        .getResult();
            }
        }

        filterAllColumns(combinedPredicate);
    }
}

void DBProgramGenerator::generateLabelConstraints(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    using NodeDataPair = std::pair<const NodePattern*, const NodePatternData*>;
    std::vector<NodeDataPair> labelConstrainedNodes;

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();

        labelConstrainedNodes.clear();

        for (const PatternElement* element : pattern->elements()) {
            const NodePattern* rootNode = static_cast<const NodePattern*>(element->getRootEntity());
            const NodePatternData* rootData = rootNode->getData();

            if (rootData && !rootData->labelConstraints().empty()) {
                labelConstrainedNodes.emplace_back(rootNode, rootData);
            }

            for (auto [edgePattern, nodePattern] : element->getElementChain()) {
                const NodePatternData* nodeData = nodePattern->getData();
                if (nodeData && !nodeData->labelConstraints().empty()) {
                    labelConstrainedNodes.emplace_back(nodePattern, nodeData);
                }
            }
        }

        if (labelConstrainedNodes.empty()) {
            continue;
        }

        const mlir::Location loc = _opBuilder.getUnknownLoc();
        const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
        const mlir::db::ColumnType labelSetIDType = allocColumnType(
            mlir::storage::LabelSetIDType::get(_mlirCtxt));

        mlir::Value combinedPredicate;

        for (auto [nodePattern, nodeData] : labelConstrainedNodes) {
            const VarDecl* vardecl = nodePattern->getDecl();
            bioassert(vardecl, "Null variable declaration.");
            const std::string_view nodeName = vardecl->getName();

            mlir::Value nodeColumn;
            for (const auto& [var, values] : _varMap) {
                if (var->getName() == nodeName) {
                    nodeColumn = values.back();
                    break;
                }
            }

            bioassert(nodeColumn, "Label-constrained node not found in variable map: {}", nodeName);

            const mlir::Value labelSetIDColumn = _opBuilder.create<mlir::db::GetNodeLabelSet>(
                loc,
                labelSetIDType,
                nodeColumn).getResult();

            llvm::SmallVector<llvm::StringRef> labelNames;
            for (const std::string_view label : nodeData->labelConstraints()) {
                labelNames.push_back(llvm::StringRef(label.data(), label.size()));
            }

            const mlir::ArrayAttr labelsAttr = _opBuilder.getStrArrayAttr(labelNames);
            const mlir::Value labelMask = _opBuilder.create<mlir::db::CheckLabelConstraint>(
                loc,
                boolType,
                labelSetIDColumn,
                labelsAttr).getResult();

            if (!combinedPredicate) {
                combinedPredicate = labelMask;
            } else {
                combinedPredicate = _opBuilder.create<mlir::db::AndOp>(
                    loc, boolType, combinedPredicate, labelMask).getResult();
            }
        }

        filterAllColumns(combinedPredicate);
    }
}

void DBProgramGenerator::generateEdgeTypeConstraints(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    const VariableDependencyGraph::EdgeIdentityMap& edgeIdentities = _vdg.edgeIdentities();

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();

        mlir::Value combinedPredicate;

        llvm::SmallVector<llvm::StringRef> typeNames;
        for (const PatternElement* element : pattern->elements()) {
            for (auto [edgePattern, nodePattern] : element->getElementChain()) {
                const EdgePatternData* edgeData = edgePattern->getData();
                if (!edgeData || edgeData->edgeTypeConstraints().empty()) {
                    continue;
                }

                const std::string_view cypherName = edgePattern->getDecl()->getName();
                const auto identityIt = edgeIdentities.find(std::string(cypherName));
                bioassert(identityIt != edgeIdentities.end(),
                          "Edge variable not found in identity map: {}", cypherName);

                const VariableDependency* edgeVar = identityIt->second.front();
                const auto edgeTypeIt = _edgeTypeMap.find(edgeVar);
                bioassert(edgeTypeIt != _edgeTypeMap.end(),
                          "Edge type column not found for variable: {}", cypherName);

                const mlir::Value edgeTypeColumn = edgeTypeIt->second;

                const mlir::Location loc = _opBuilder.getUnknownLoc();
                const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

                typeNames.clear();
                for (const std::string_view typeName : edgeData->edgeTypeConstraints()) {
                    typeNames.push_back(llvm::StringRef(typeName.data(), typeName.size()));
                }

                const mlir::ArrayAttr edgeTypesAttr = _opBuilder.getStrArrayAttr(typeNames);
                const mlir::Value edgeTypeMask = _opBuilder.create<mlir::db::CheckEdgeTypeConstraint>(
                    loc, boolType, edgeTypeColumn, edgeTypesAttr).getResult();

                if (!combinedPredicate) {
                    combinedPredicate = edgeTypeMask;
                } else {
                    combinedPredicate = _opBuilder.create<mlir::db::AndOp>(
                        loc, boolType, combinedPredicate, edgeTypeMask).getResult();
                }
            }
        }

        if (combinedPredicate) {
            filterAllColumns(combinedPredicate);
        }
    }
}

void DBProgramGenerator::generateFilters(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        throw TuringException("Multiple queries not yet supported.");
    }

    const QueryCommand* query = queries.front();

    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const StmtContainer* stmtsContainer = sglPart->getReadStmts();
    if (!stmtsContainer) {
        return;
    }

    for (const Stmt* stmt : stmtsContainer->stmts()) {
        if (stmt->getKind() != Stmt::Kind::MATCH) {
            continue;
        }

        const MatchStmt* matchStmt = static_cast<const MatchStmt*>(stmt);
        const Pattern* pattern = matchStmt->getPattern();
        const WhereClause* where = pattern->getWhere();
        if (!where) {
            continue;
        }

        const Expr* predicateExpr = where->getExpr();
        translateExpr(predicateExpr);

        const auto findIt = _exprMap.find(predicateExpr);
        bioassert(findIt != end(_exprMap), "Failed to get value for expr");
        const mlir::Value predicateVal = findIt->second;

        filterAllColumns(predicateVal);
    }
}

void DBProgramGenerator::translateExpr(const Expr* expr) {
    if (_exprMap.contains(expr)) {
        return;
    }

    const Expr::Kind kind = expr->getKind();
    switch (kind) {
        case Expr::Kind::PROPERTY: {
            const PropertyExpr* propExpr = static_cast<const PropertyExpr*>(expr);
            _exprMap[expr] = translatePropertyExpr(propExpr);
        }
        break;

        case Expr::Kind::LITERAL: {
            const LiteralExpr* litExpr = static_cast<const LiteralExpr*>(expr);
            _exprMap[expr] = translateLiteralExpr(litExpr->getLiteral());
        }
        break;

        case Expr::Kind::BINARY: {
            const BinaryExpr* binExpr = static_cast<const BinaryExpr*>(expr);
            translateBinaryExpr(expr, binExpr);
        }
        break;

        case Expr::Kind::SYMBOL: {
            const SymbolExpr* symbolExpr = static_cast<const SymbolExpr*>(expr);
            const std::string_view varName = symbolExpr->getDecl()->getName();

            for (const auto& [var, values] : _varMap) {
                if (var->getName() == varName) {
                    _exprMap[expr] = values.back();
                    break;
                }
            }

            bioassert(_exprMap.contains(expr), "Symbol refers to unknown variable: {}", varName);
        }
        break;

        case Expr::Kind::UNARY: {
            const UnaryExpr* unaryExpr = static_cast<const UnaryExpr*>(expr);
            translateUnaryExpr(expr, unaryExpr);
        }
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(expr);
            translateFunctionInvocationExpr(expr, funcExpr);
        }
        break;

        case Expr::Kind::INDEX:
        case Expr::Kind::LIST:
        case Expr::Kind::STRING:
        case Expr::Kind::ENTITY_TYPES:
        case Expr::Kind::PATH:
            throw TuringException(fmt::format("Unsupported expression: {}",
                                              ExprKindDescription::value(kind)));
        break;

        case Expr::Kind::_SIZE:
            throw FatalException("Invalid expression kind.");
        break;
    }
}

void DBProgramGenerator::translateUnaryExpr(const Expr* expr, const UnaryExpr* unaryExpr) {
    const UnaryOperator op = unaryExpr->getOperator();
    const Expr* subExpr = unaryExpr->getSubExpr();
    translateExpr(subExpr);
    bioassert(_exprMap.contains(subExpr), "Unary operation with unknown operand.");
    const mlir::Value operandVal = _exprMap.at(subExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    switch (op) {
        case UnaryOperator::Not: {
            const mlir::db::ColumnType boolType =
                allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
            auto notOp = _opBuilder.create<mlir::db::NotOp>(loc, boolType, operandVal);
            const mlir::Value result = notOp.getResult();
            _exprMap[expr] = result;
        }
        break;

        case UnaryOperator::Minus:
        case UnaryOperator::Plus:
            throw TuringException(fmt::format("Unsupported unary operator: {}",
                                              UnaryOperatorDescription::value(op)));
        break;

        case UnaryOperator::_SIZE:
            throw TuringException("Unknown unary operator.");
        break;
    }
}

void DBProgramGenerator::translateBinaryExpr(const Expr* expr, const BinaryExpr* binExpr) {
    const Expr* lhsExpr = binExpr->getLHS();
    const Expr* rhsExpr = binExpr->getRHS();

    translateExpr(lhsExpr);
    translateExpr(rhsExpr);

    bioassert(_exprMap.contains(lhsExpr), "Binary operation with unknown LHS operand.");
    bioassert(_exprMap.contains(rhsExpr), "Binary operation with unknown RHS operand.");

    const mlir::Value lhs = _exprMap.at(lhsExpr);
    const mlir::Value rhs = _exprMap.at(rhsExpr);
    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const BinaryOperator op = binExpr->getOperator();

    switch (op) {
        case BinaryOperator::Equal:
            _exprMap[expr] = _opBuilder.create<mlir::db::EqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::And:
            _exprMap[expr] = _opBuilder.create<mlir::db::AndOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Or:
            _exprMap[expr] = _opBuilder.create<mlir::db::OrOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Add:
            _exprMap[expr] = _opBuilder.create<mlir::db::AddOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Sub:
            _exprMap[expr] = _opBuilder.create<mlir::db::SubOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mult:
            _exprMap[expr] = _opBuilder.create<mlir::db::MulOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Div:
            _exprMap[expr] = _opBuilder.create<mlir::db::DivOp>(loc, noneType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThan:
            _exprMap[expr] = _opBuilder.create<mlir::db::GtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThan:
            _exprMap[expr] = _opBuilder.create<mlir::db::LtOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::GreaterThanOrEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::GteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::LessThanOrEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::LteOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::NotEqual:
            _exprMap[expr] = _opBuilder.create<mlir::db::NeqOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Xor:
            _exprMap[expr] = _opBuilder.create<mlir::db::XorOp>(loc, boolType, lhs, rhs).getResult();
        break;
        case BinaryOperator::Mod:
        case BinaryOperator::Pow:
        case BinaryOperator::In:
            throw TuringException(fmt::format("Unsupported operation: {}",
                                              BinaryOperatorDescription::value(op)));
        break;

        case BinaryOperator::_SIZE:
        break;
    }
}

mlir::Value DBProgramGenerator::translateLiteralExpr(const Literal* literal) {
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    mlir::TypedAttr valueAttr;

    switch (literal->getKind()) {
        case Literal::Kind::BOOL: {
            const BoolLiteral* boolLiteral = static_cast<const BoolLiteral*>(literal);
            valueAttr = _opBuilder.getBoolAttr(boolLiteral->getValue());
        }
        break;
        case Literal::Kind::INTEGER: {
            const IntegerLiteral* intLiteral = static_cast<const IntegerLiteral*>(literal);
            valueAttr = _opBuilder.getI64IntegerAttr(intLiteral->getValue());
        }
        break;
        case Literal::Kind::DOUBLE: {
            const DoubleLiteral* doubleLiteral = static_cast<const DoubleLiteral*>(literal);
            valueAttr = _opBuilder.getF64FloatAttr(doubleLiteral->getValue());
        }
        break;

        case Literal::Kind::STRING: {
            const StringLiteral* stringLiteral = static_cast<const StringLiteral*>(literal);
            const mlir::Type stringType = mlir::storage::StringType::get(_mlirCtxt);
            valueAttr = mlir::StringAttr::get(stringLiteral->getValue(), stringType);
        }
        break;

        case Literal::Kind::EMBEDDING: {
            const EmbeddingLiteral* embeddingLiteral = static_cast<const EmbeddingLiteral*>(literal);
            const std::span<const float> floats = embeddingLiteral->getValue();
            const mlir::FloatType f32Type = _opBuilder.getF32Type();
            const size_t size = floats.size();

            const mlir::RankedTensorType tensorType = mlir::RankedTensorType::get(size, f32Type);

            const auto* bytes = reinterpret_cast<const char*>(floats.data());
            const size_t numBytes = size * sizeof(float);

            const llvm::ArrayRef<char> rawBytes {bytes, numBytes};

            const mlir::TypedAttr embAttr = mlir::DenseElementsAttr::getFromRawBuffer(tensorType, rawBytes);
            const mlir::db::ColumnType embResultType = allocColumnType(mlir::storage::EmbeddingType::get(_mlirCtxt));
            return _opBuilder.create<mlir::db::ConstantOp>(uloc, embResultType, embAttr).getResult();
        }
        break;

        case Literal::Kind::NULL_LITERAL: {
            const mlir::Type nullableType = mlir::storage::NullableType::get(
                _mlirCtxt, mlir::NoneType::get(_mlirCtxt));
            valueAttr = mlir::StringAttr::get("", nullableType);
        }
        break;
        case Literal::Kind::LIST:
            throw FatalException("List literals are not yet supported in MLIR codegen.");
        break;

        default:
            throw FatalException("Unsupported literal kind in WHERE clause expression.");
        break;
    }

    const mlir::db::ColumnType resultType = allocColumnType(valueAttr.getType());
    return _opBuilder.create<mlir::db::ConstantOp>(uloc, resultType, valueAttr).getResult();
}

mlir::Value DBProgramGenerator::translatePropertyExpr(const PropertyExpr* propExpr) {
    const VarDecl* entityDecl = propExpr->getEntityVarDecl();
    const std::string_view varName = entityDecl->getName();
    const std::string_view propName = propExpr->getPropName();

    const mlir::Value entityColumn = resolveEntityColumn(varName);

    bioassert(entityColumn, "WHERE clause property access on unknown variable: {}", varName);

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    // Use None for property type and infer during lowering
    const mlir::db::ColumnType resultType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::StringAttr propAttr = _opBuilder.getStringAttr(propName);
    const EvaluatedType entityType = entityDecl->getType();

    const bool isNode = entityType == EvaluatedType::NodePattern;
    const bool isEdge = entityType == EvaluatedType::EdgePattern;
    bioassert(isNode || isEdge, "Property access on non-entity variable: {}", varName);

    if (isNode) {
        auto op = _opBuilder.create<mlir::db::GetNodeProperties>(loc, resultType, entityColumn, propAttr);
        return op.getResult();
    } else {
        auto op = _opBuilder.create<mlir::db::GetEdgeProperties>(loc, resultType, entityColumn, propAttr);
        return op.getResult();
    }
}

void DBProgramGenerator::translateFunctionInvocationExpr(const Expr* expr,
                                                         const FunctionInvocationExpr* funcExpr) {
    const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
    const std::string_view funcName = invocation->getSignature()->getFullName();

    if (!funcExpr->isAggregate()) {
        throw TuringException("Non-aggregate function invocations are not yet supported");
    }

    const mlir::Location loc = _opBuilder.getUnknownLoc();
    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));

    const ExprChain* args = invocation->getArguments();
    bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

    const Expr* argExpr = args->front();
    mlir::Value inputColumn;

    if (argExpr->getType() == EvaluatedType::Wildcard) {
        bioassert(!_varMap.empty(), "count(*) requires at least one traversal variable.");
        inputColumn = _varMap.begin()->second.back();
    } else {
        translateExpr(argExpr);
        inputColumn = _exprMap.at(argExpr);
    }

    if (funcName == "count") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Count>(loc, noneType, inputColumn).getResult();
    } else if (funcName == "sum") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Sum>(loc, noneType, inputColumn).getResult();
    } else if (funcName == "min") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Min>(loc, noneType, inputColumn).getResult();
    } else if (funcName == "max") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Max>(loc, noneType, inputColumn).getResult();
    } else if (funcName == "avg") {
        _exprMap[expr] = _opBuilder.create<mlir::db::Avg>(loc, noneType, inputColumn).getResult();
    } else {
        throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
    }
}

void DBProgramGenerator::generateGroupAggregate(const CypherAST* ast) {
    const CypherAST::QueryCommands& queries = ast->queries();
    if (queries.size() != 1) {
        return;
    }

    const QueryCommand* query = queries.front();
    const SinglePartQuery* sglPart = dynamic_cast<const SinglePartQuery*>(query);
    if (!sglPart) {
        return;
    }

    const ReturnStmt* rtn = sglPart->getReturnStmt();
    if (!rtn) {
        return;
    }

    const Projection* proj = rtn->getProjection();
    if (!proj->isAggregate() || !proj->hasGroupingKeys()) {
        return;
    }

    FinalIdentityMap finalIdentities;
    for (const auto& [cypherVar, mlirCol] : _varMap) {
        finalIdentities[cypherVar->getName()] = mlirCol.back();
    }

    for (const auto& [name, vars] : _vdg.edgeIdentities()) {
        if (!vars.empty() && _varMap.contains(vars.front())) {
            finalIdentities[name] = _varMap.at(vars.front()).back();
        }
    }

    // Parallel: for each key position, exactly one of these is non-null
    llvm::SmallVector<mlir::Value> keyColumns;
    llvm::SmallVector<const VariableDependency*> keyVarAtPos;
    llvm::SmallVector<const Expr*> keyExprAtPos;

    llvm::SmallVector<mlir::Value> aggInputColumns;
    llvm::SmallVector<int64_t> aggKinds;
    llvm::SmallVector<const FunctionInvocationExpr*> aggFuncExprs;

    const mlir::db::ColumnType noneType = allocColumnType(mlir::NoneType::get(_mlirCtxt));
    const mlir::Location loc = _opBuilder.getUnknownLoc();

    for (const Projection::ReturnItem& returnItem : proj->items()) {
        if (const VarDecl* const* varDeclPtr = std::get_if<VarDecl*>(&returnItem)) {
            const std::string_view name = (*varDeclPtr)->getName();
            const auto findIt = finalIdentities.find(name);
            bioassert(findIt != finalIdentities.end(), "Grouping key variable {} not found.", name);
            keyColumns.push_back(findIt->second);

            const VariableDependency* keyVar = nullptr;
            for (auto& [var, values] : _varMap) {
                if (var->getName() == name) {
                    keyVar = var;
                    break;
                }
            }
            keyVarAtPos.push_back(keyVar);
            keyExprAtPos.push_back(nullptr);
            continue;
        }

        const Expr* item = std::get<Expr*>(returnItem);

        if (!item->isAggregate()) {
            keyColumns.push_back(resolveExprColumn(finalIdentities, item));
            keyVarAtPos.push_back(nullptr);
            keyExprAtPos.push_back(item);
            continue;
        }

        const FunctionInvocationExpr* funcExpr = static_cast<const FunctionInvocationExpr*>(item);
        const FunctionInvocation* invocation = funcExpr->getFunctionInvocation();
        const std::string_view funcName = invocation->getSignature()->getFullName();

        int64_t kind;
        if (funcName == "count") {
            kind = 0;
        } else if (funcName == "sum") {
            kind = 1;
        } else if (funcName == "min") {
            kind = 2;
        } else if (funcName == "max") {
            kind = 3;
        } else if (funcName == "avg") {
            kind = 4;
        } else {
            throw TuringException(fmt::format("Unsupported aggregate function: {}", funcName));
        }

        const ExprChain* args = invocation->getArguments();
        bioassert(args && !args->empty(), "Aggregate function invocation with no arguments.");

        const Expr* argExpr = args->front();
        mlir::Value inputColumn;

        if (argExpr->getType() == EvaluatedType::Wildcard) {
            bioassert(!_varMap.empty(), "count(*) requires at least one traversal variable.");
            inputColumn = _varMap.begin()->second.back();
        } else {
            translateExpr(argExpr);
            inputColumn = _exprMap.at(argExpr);
        }

        aggInputColumns.push_back(inputColumn);
        aggKinds.push_back(kind);
        aggFuncExprs.push_back(funcExpr);
    }

    const size_t keyCount = keyColumns.size();
    const size_t aggCount = aggInputColumns.size();
    bioassert(keyCount > 0, "grouped aggregate with no key columns.");
    bioassert(aggCount > 0, "grouped aggregate with no aggregate columns.");

    llvm::SmallVector<mlir::Value> allColumns;
    for (const mlir::Value col : keyColumns) {
        allColumns.push_back(col);
    }
    for (const mlir::Value col : aggInputColumns) {
        allColumns.push_back(col);
    }

    llvm::SmallVector<mlir::Type> resultTypes;
    for (const mlir::Value col : keyColumns) {
        resultTypes.push_back(col.getType());
    }
    for (size_t i = 0; i < aggCount; i++) {
        resultTypes.push_back(noneType);
    }

    auto groupAgg = _opBuilder.create<mlir::db::GroupAggregate>(
        loc,
        mlir::TypeRange{resultTypes},
        mlir::ValueRange{allColumns},
        static_cast<uint64_t>(keyCount),
        llvm::ArrayRef<int64_t>{aggKinds});

    const mlir::ResultRange results = groupAgg.getResults();

    for (size_t i = 0; i < keyCount; i++) {
        if (keyVarAtPos[i]) {
            registerValue(keyVarAtPos[i], results[i]);
        } else {
            _exprMap[keyExprAtPos[i]] = results[i];

            const bool isSymbol = keyExprAtPos[i]->getKind() == Expr::Kind::SYMBOL;
            if (not isSymbol) {
                continue;
            }

            // Symbols need their value updated: the aggregate gives them a new value
            const SymbolExpr* sym = static_cast<const SymbolExpr*>(keyExprAtPos[i]);
            const std::string_view symName = sym->getDecl()->getName();
            for (auto& [var, values] : _varMap) {
                if (var->getName() == symName) {
                    registerValue(var, results[i]);
                    break;
                }
            }
        }
    }

    for (size_t i = 0; i < aggCount; i++) {
        _exprMap[aggFuncExprs[i]] = results[keyCount + i];
    }
}
