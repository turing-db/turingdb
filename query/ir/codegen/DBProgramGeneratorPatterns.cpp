#include "DBProgramGenerator.h"

#include <algorithm>
#include <optional>
#include <span>
#include <string_view>
#include <type_traits>
#include <variant>
#include <vector>

#include "LabelExpr.h"

#include "mlir/IR/Builders.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/STLExtras.h"

#include "DBOps.h"
#include "DBTypes.h"
#include "DBBoundColumns.h"
#include "StorageTypes.h"
#include "IRConstantColumn.h"
#include "IRLiteralList.h"

#include "DependencyEdge.h"
#include "EdgeMetadata.h"
#include "VariableDependency.h"
#include "VariableDependencyGraph.h"

#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "Literal.h"
#include "expr/LiteralExpr.h"
#include "stmt/UnwindStmt.h"

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

}

void DBProgramGenerator::addScanNodes(const VariableDependency* var) {
    bioassert(!_part._varMap.contains(var), "ScanNodes for registered variable");

    const auto col = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    auto scan = _opBuilder.create<mlir::db::ScanNodes>(_opBuilder.getUnknownLoc(), col);

    registerValue(var, scan.getResult());
}

void DBProgramGenerator::addYieldedColumn(const VariableDependency* var, mlir::Value column) {
    bioassert(!_part._varMap.contains(var), "Yielded column for registered variable");

    registerValue(var, column);

    // The variable owns those rows now. Leaving them among the yields as well would give one
    // column two names, and only the variable's is rebound as the expansion replicates it.
    const auto declaresVar = [&](const YieldedColumn& yielded) {
        return yielded._decl == var->getDecl();
    };

    std::erase_if(_part._yieldedColumns, declaresVar);
}

void DBProgramGenerator::addConstScanNodes(const VariableDependency* var, const UnwindStmt* unwind) {
    bioassert(!_part._varMap.contains(var), "ConstScanNodes for registered variable");

    llvm::SmallVector<mlir::Attribute> elements;
    translateUnwoundElements(unwind, elements);

    llvm::SmallVector<int64_t> nodeIDs;
    nodeIDs.reserve(elements.size());

    for (const mlir::Attribute element : elements) {
        const mlir::IntegerAttr nodeID = mlir::dyn_cast<mlir::IntegerAttr>(element);
        if (!nodeID) {
            throwError("Only node IDs can be unwound into a node pattern.", unwind);
        }

        nodeIDs.push_back(nodeID.getInt());
    }

    const mlir::db::ColumnType nodeColumnType = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    mlir::db::ConstScanNodes constScan = _opBuilder.create<mlir::db::ConstScanNodes>(_opBuilder.getUnknownLoc(),
                                                                                    nodeColumnType,
                                                                                    nodeIDs);

    registerValue(var, constScan.getResult());
    _part._seededVars.insert(var);
}

void DBProgramGenerator::addUnwindConst(const VariableDependency* var, const UnwindStmt* unwind) {
    bioassert(!_part._varMap.contains(var), "UnwindConst for registered variable");

    llvm::SmallVector<mlir::Attribute> elements;
    translateUnwoundElements(unwind, elements);

    const mlir::ArrayAttr elementsAttr = _opBuilder.getArrayAttr(elements);

    const mlir::Type sharedType = sharedLiteralElementType(elementsAttr);
    const mlir::Type elementType = mlir::isa<mlir::NoneType>(sharedType) ? mlir::storage::AnyType::get(_mlirCtxt)
                                                                         : sharedType;

    const mlir::db::ColumnType resultType = allocColumnType(elementType);

    mlir::db::UnwindConst unwindConst = _opBuilder.create<mlir::db::UnwindConst>(_opBuilder.getUnknownLoc(),
                                                                                resultType,
                                                                                elementsAttr);

    registerValue(var, unwindConst.getResult());
}

void DBProgramGenerator::translateUnwoundElements(const UnwindStmt* unwind,
                                                  llvm::SmallVectorImpl<mlir::Attribute>& elements) {
    const LiteralExpr* literalExpr = dynamic_cast<const LiteralExpr*>(unwind->arg());
    if (!literalExpr) {
        throwError("Non-literal UNWIND expressions are not yet supported.", unwind);
    }

    const Literal* literal = literalExpr->getLiteral();
    const Literal::Kind kind = literal->getKind();

    if (kind == Literal::Kind::LIST) {
        translateListElements(static_cast<const ListLiteral*>(literal), elements);
    } else if (kind != Literal::Kind::NULL_LITERAL) {
        elements.push_back(listElementAttr(literal));
    }
}

template<typename EdgeOp>
void DBProgramGenerator::addEdgeTraversal(const VariableDependency* src,
                                          const VariableDependency* edge,
                                          const VariableDependency* tgt,
                                          const std::vector<const VariableDependency*>& carrySet) {
    walkEdge<EdgeOp>(src, edge, tgt, carrySet, nullptr);
}

template<typename EdgeOp>
mlir::Value DBProgramGenerator::addJoiningEdgeTraversal(const VariableDependency* src,
                                                        const VariableDependency* edge,
                                                        const VariableDependency* tgt,
                                                        const std::vector<const VariableDependency*>& carrySet) {
    mlir::Value landed;
    walkEdge<EdgeOp>(src, edge, tgt, carrySet, &landed);

    return landed;
}

template<typename EdgeOp>
void DBProgramGenerator::walkEdge(const VariableDependency* src,
                                  const VariableDependency* edge,
                                  const VariableDependency* tgt,
                                  const std::vector<const VariableDependency*>& carrySet,
                                  mlir::Value* joinedTarget) {
    static_assert(std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetInEdges>
                      or std::is_same_v<EdgeOp, mlir::db::GetEdges>, "Invalid op");

    bioassert(src, "Null source");
    bioassert(tgt, "Null target");

    const auto srcs = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));
    const auto eids = allocColumnType(mlir::storage::EdgeIDType::get(_mlirCtxt));
    const auto etypes = allocColumnType(mlir::storage::EdgeTypeIDType::get(_mlirCtxt));
    const auto tgts = allocColumnType(mlir::storage::NodeIDType::get(_mlirCtxt));

    const mlir::Value input = findVarOrThrow(_part._varMap, src);

    InFlightColumns carried;
    collectHopCarrySet(src, carrySet, carried);

    llvm::SmallVector<mlir::Value> operands {input};
    llvm::SmallVector<mlir::Type> results {srcs, eids, etypes, tgts};
    for (const mlir::Value column : carried._columns) {
        operands.push_back(column);
        results.push_back(column.getType());
    }

    const auto loc = _opBuilder.getUnknownLoc();
    auto op = _opBuilder.create<EdgeOp>(loc, results, operands);

    const mlir::Value newSrcs = op.getResult(0);
    const mlir::Value newEdges = op.getResult(1);
    const mlir::Value newEtypes = op.getResult(2);
    const mlir::Value newTgts = op.getResult(3);

    _part._edgeTypeMap[edge] = newEtypes;

    constexpr bool forwardOrientation = std::is_same_v<EdgeOp, mlir::db::GetOutEdges>
                                        or std::is_same_v<EdgeOp, mlir::db::GetEdges>;
    const mlir::Value sourceColumn = forwardOrientation ? newSrcs : newTgts;
    const mlir::Value targetColumn = forwardOrientation ? newTgts : newSrcs;

    registerValue(src, sourceColumn);
    registerValue(edge, newEdges);

    if (joinedTarget) {
        *joinedTarget = targetColumn;
    } else {
        registerValue(tgt, targetColumn);
    }

    rebindInFlightColumns(op.getResults(), 4, carried);
}

void DBProgramGenerator::collectHopCarrySet(const VariableDependency* src,
                                            const std::vector<const VariableDependency*>& carrySet,
                                            InFlightColumns& inFlight) {
    for (const VariableDependency* var : carrySet) {
        // source variable is explicitly filtered by the edge op
        if (var == src) {
            continue;
        }

        inFlight._columns.push_back(findVarOrThrow(_part._varMap, var));
        inFlight._variables.push_back(var);
    }

    // A column a barrier published rides the expansion too, when the traversal it drives is
    // generated where that column was bound: the hop replicates a row once per edge, so it
    // comes along or it stops matching the rows beside it. A published pattern variable is
    // walked by the component instead and is already in the carry set.
    for (const VariableDependency* var : _vdg.boundVars()) {
        const bool walkedHere = var == src || llvm::is_contained(carrySet, var);
        if (walkedHere) {
            continue;
        }

        const mlir::Value column = findVarOrThrow(_part._varMap, var);
        if (!isRowAlignedHere(column) || yieldsConstantColumn(column, _constantColumns)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._variables.push_back(var);
    }

    // Find the edge types to carry which were defined in this block
    for (auto& [edgeVar, column] : _part._edgeTypeMap) {
        if (!isRowAlignedHere(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._edgeTypeVariables.push_back(edgeVar);
    }

    // A column a CALL driving this traversal yielded is in flight here too: the expansion
    // replicates a row once per edge, so it comes along or it stops matching the rows beside
    // it. Nothing yielded is in flight when a call has yet to run, which is every traversal
    // the calls do not drive.
    for (size_t yieldedIndex = 0; yieldedIndex < _part._yieldedColumns.size(); yieldedIndex++) {
        const mlir::Value column = _part._yieldedColumns[yieldedIndex]._column;
        if (!isRowAlignedHere(column)) {
            continue;
        }

        inFlight._columns.push_back(column);
        inFlight._yieldedIndices.push_back(yieldedIndex);
    }
}

bool DBProgramGenerator::isValidRoot(const VariableDependency& var) const {
    const auto isEdgeTgtMetaVar = [](const DependencyEdge* e) -> bool {
        return e->isMetaEdge();
    };

    // A valid root is a non-meta Cypher variable which is a node
    return std::ranges::none_of(var.incoming(), [&](const DependencyEdge* e) {
        return producesEdgeVar(e) || isEdgeTgtMetaVar(e);
    });
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
    const mlir::Value fstSourceCol = findVarOrThrow(_part._varMap, fstMergeSource);
    const mlir::Value sndSourceCol = findVarOrThrow(_part._varMap, sndMergeSource);
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    // Create an EQ op to keep only rows where both sources are the same
    auto eq =
        _opBuilder.create<mlir::db::EqOp>(uloc, boolType, fstSourceCol, sndSourceCol);
    const mlir::Value eqRes = eq.getResult();

    // Register mergeVar's initial value (first source, arbitrary) so filterAllColumns
    // picks it up along with the rest of the part's variables.
    registerValue(mergeVar, fstSourceCol);
    filterAllColumns(eqRes);
    carriedSet.push_back(mergeVar);
}

void DBProgramGenerator::extendBoundDataflow(DefinedVars& defined,
                                             std::vector<const VariableDependency*>& dataflowVars) {
    const VariableDependencyGraph::BoundVars& bound = _vdg.boundVars();

    // Every column the barrier published already holds its rows, so no scan opens a
    // dataflow here: the hops of this part extend that one, carrying those columns along.
    // A constant has no rows to carry and rides along untouched
    std::vector<const VariableDependency*> carriedSet;
    for (const VariableDependency* var : bound) {
        defined.insert(var);

        if (!yieldsConstantColumn(findVarOrThrow(_part._varMap, var), _constantColumns)) {
            carriedSet.push_back(var);
            dataflowVars.push_back(var);
        }
    }

    for (const VariableDependency* var : bound) {
        applyConstraints(var);
        filterNullNode(var);
    }

    for (const VariableDependency* var : bound) {
        expandComponent(var, defined, carriedSet, dataflowVars);
    }

    closeBoundJoins(carriedSet, dataflowVars);
}

void DBProgramGenerator::closeBoundJoins(std::vector<const VariableDependency*>& carriedSet,
                                         std::vector<const VariableDependency*>& dataflowVars) {
    // The walk orients a hop by the one end of it that is not yet bound, so it leaves
    // behind the hops whose ends were both bound already - a pattern reaching back to a
    // variable the barrier published, which constrains the rows rather than fanning them out
    for (const VariableDependency& var : _vdg.vars()) {
        if (holdsColumn(&var)) {
            continue;
        }

        const VariableDependency::Edges& incoming = var.incoming();
        const VariableDependency::Edges& outgoing = var.outgoing();

        const auto edgeProducerIt = std::ranges::find_if(incoming, producesEdgeVar);
        const auto nodeProducerIt = std::ranges::find_if(outgoing, producesNodeVar);

        const bool isPatternEdge = edgeProducerIt != end(incoming) && nodeProducerIt != end(outgoing);
        if (!isPatternEdge) {
            continue;
        }

        const VariableDependency* target = (*nodeProducerIt)->tgt();

        // A hop left behind with an end still open is not a join but a pattern of its own,
        // which this walk does not reach: only both ends holding a column already make the
        // hop a constraint on the rows they were bound over
        const bool closesAJoin = holdsColumn((*edgeProducerIt)->src()) && holdsColumn(target);
        if (!closesAJoin) {
            continue;
        }

        closeBoundJoin(*edgeProducerIt, &var, target, carriedSet, dataflowVars);
    }
}

void DBProgramGenerator::closeBoundJoin(const DependencyEdge* edgeProducer,
                                        const VariableDependency* edge,
                                        const VariableDependency* target,
                                        std::vector<const VariableDependency*>& carriedSet,
                                        std::vector<const VariableDependency*>& dataflowVars) {
    const VariableDependency* source = edgeProducer->src();

    mlir::Value landed;
    const EdgeMetadata& metadata = edgeProducer->data();
    const EdgeMetadata::EdgeType direction = metadata.type();

    if (metadata.isQuantified()) {
        addExplorePaths(source, edge, target, carriedSet, metadata, /*reversed=*/false, &landed);
    } else {
        switch (direction) {
            case EdgeMetadata::EdgeType::GET_OUT_EDGES:
                landed = addJoiningEdgeTraversal<mlir::db::GetOutEdges>(source, edge, target, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_IN_EDGES:
                landed = addJoiningEdgeTraversal<mlir::db::GetInEdges>(source, edge, target, carriedSet);
            break;

            case EdgeMetadata::EdgeType::GET_EDGES:
                landed = addJoiningEdgeTraversal<mlir::db::GetEdges>(source, edge, target, carriedSet);
            break;

            default:
                throw FatalException(fmt::format("Attempted to close a join over {}",
                                                 EdgeTypeName::value(direction)));
            break;
        }
    }

    const mlir::Value bound = findVarOrThrow(_part._varMap, target);
    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));

    auto eq = _opBuilder.create<mlir::db::EqOp>(_opBuilder.getUnknownLoc(), boolType, landed, bound);
    filterAllColumns(eq.getResult());

    if (!metadata.isQuantified()) {
        applyConstraints(edge);
    }

    carriedSet.push_back(edge);

    // The walk lists an edge as soon as it reaches it, before any hop has given it a
    // column, so one whose ends were both bound is already there with nothing behind it
    if (std::ranges::find(dataflowVars, edge) == dataflowVars.end()) {
        dataflowVars.push_back(edge);
    }
}

void DBProgramGenerator::openComponent(const VariableDependency* root) {
    // A root's dataflow opens with whatever already holds its rows: the column a CALL bound
    // it to, the literal list an UNWIND binds it to, or - a pattern variable nothing has
    // bound yet - a scan of the graph's nodes.
    const VariableDependencyGraph::UnwindSourceMap& unwindSources = _vdg.unwindSources();
    const auto unwindIt = unwindSources.find(root);
    const mlir::Value yieldedColumn = findYieldedColumn(root->getDecl());

    if (yieldedColumn) {
        addYieldedColumn(root, yieldedColumn);
        filterNullNode(root);
    } else if (unwindIt != unwindSources.end()) {
        const VarDecl* const decl = root->getDecl();
        const bool seedsAPatternNode = decl && decl->getType() == EvaluatedType::NodePattern;

        // A pattern naming the unwound variable makes its list a set of node IDs rather
        // than a column of values, so the nodes themselves open the dataflow.
        if (seedsAPatternNode) {
            addConstScanNodes(root, unwindIt->second);
        } else {
            addUnwindConst(root, unwindIt->second);
        }
    } else {
        addScanNodes(root);
    }

    applyConstraints(root);
}

void DBProgramGenerator::translateComponent(const VariableDependency* root,
                                            DefinedVars& defined,
                                            std::vector<const VariableDependency*>& outVars) {
    openComponent(root);

    // Forms the "carried set" for this connected component
    std::vector<const VariableDependency*> carriedSet;

    expandComponent(root, defined, carriedSet, outVars);

    closeBoundJoins(carriedSet, outVars);
}

void DBProgramGenerator::expandCrossedComponents(llvm::ArrayRef<const VariableDependency*> roots,
                                                 const std::vector<TranslatedComponent>& components,
                                                 DefinedVars& defined) {
    std::vector<const VariableDependency*> carriedSet;
    for (const TranslatedComponent& component : components) {
        carriedSet.insert(carriedSet.end(), component._vars.begin(), component._vars.end());
    }

    std::vector<const VariableDependency*> expandedVars;
    for (const VariableDependency* root : roots) {
        expandComponent(root, defined, carriedSet, expandedVars);
        closeBoundJoins(carriedSet, expandedVars);
    }
}

void DBProgramGenerator::expandComponent(const VariableDependency* root,
                                         DefinedVars& defined,
                                         std::vector<const VariableDependency*>& carriedSet,
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

    markDefined(root);

    // A merge target is defined by whatever reaches it first, and holds a column before
    // the rest of its merge does: each merge edge left open closes on a filter at the end
    std::vector<const VariableDependency*> earlyMergeTargets;
    std::unordered_set<const DependencyEdge*> mergesTaken;

    std::vector<Frame> stack;
    std::vector<Frame> deferred;

    // DFS from this root
    stack.emplace_back(root, nullptr);
    while (!stack.empty() || !deferred.empty()) {
        // A merge is taken early only once no other path is left to its other sources: a
        // hop still on the stack may be the one that gives them their column
        const bool forcesAMerge = stack.empty();
        std::vector<Frame>& frames = forcesAMerge ? deferred : stack;

        const auto [var, pred, predPred] = frames.back();
        frames.pop_back();

        const auto seenOrMeta = [&defined](const DependencyEdge* e) {
            return !e->isMetaEdge() || defined.contains(e->src());
        };
        const bool canTraverse = std::ranges::all_of(var->incoming(), seenOrMeta);

        // A merge edge joins two dataflows instead of traversing the graph, so it never
        // is half of a (source, edge, target) triple: it closes the chain it lands on,
        // and whatever leaves its target opens a new one.
        const bool predTraverses = pred && !pred->isMetaEdge();

        // Have we found a (source, edge, target) triple yet on this traversal?
        const bool haveTriple = predTraverses && predPred;

        const bool reachedOverMerge = pred && pred->isMetaEdge();
        const bool reachedFromMergeTarget = reachedOverMerge && pred->src() == var;

        const bool takesMergeEarly = reachedOverMerge && (reachedFromMergeTarget || !canTraverse);

        if (reachedOverMerge && defined.contains(var)) {
            continue;
        } else if (takesMergeEarly && !forcesAMerge) {
            deferred.emplace_back(var, pred, predPred);
            continue;
        } else if (takesMergeEarly) {
            const VariableDependency* definedEnd = reachedFromMergeTarget ? pred->tgt() : pred->src();

            registerValue(var, findVarOrThrow(_part._varMap, definedEnd));
            mergesTaken.insert(pred);
            earlyMergeTargets.push_back(var);
            carriedSet.push_back(var);
        }

        for (const DependencyEdge* e : var->edges()) {
            const VariableDependency* other = e->src() == var ? e->tgt() : e->src();
            if (defined.contains(other)) {
                continue;
            }

            if (haveTriple || !predTraverses) {
                // We have discovered a full (src, edge, tgt) triple, or @ref pred is a
                // merge edge that cannot open one: either way the next elements on the
                // stack only have (src, edge) and await tgt
                stack.emplace_back(other, e, nullptr);
            } else {
                // We have not yet discovered a full (src, edge, tgt) triple, but the
                // next elements on stack will have such a triple (with @ref pred)
                stack.emplace_back(other, e, pred);
            }
        }

        // Only translate when we have a full triple
        if (!haveTriple) {
            if (reachedOverMerge && !reachedFromMergeTarget) {
                if (!takesMergeEarly) {
                    addMergeFilter(var, carriedSet);
                }

                applyConstraints(var);
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
        const EdgeMetadata& metadata = edgeVarProd->data();
        const EdgeMetadata::EdgeType prodType = metadata.type();
        const EdgeMetadata::EdgeType logicalDir =
            edgeSrcDefined ? prodType : reverseEdge(prodType);

        if (metadata.isQuantified()) {
            addExplorePaths(src, edge, tgt, carriedSet, metadata, !edgeSrcDefined, nullptr);
        } else {
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
        }

        markDefined(src);
        markDefined(edge);
        markDefined(tgt);

        // The type of a variable-length hop filters inside the exploration; a path has
        // no single edge type column to check afterwards
        if (!metadata.isQuantified()) {
            applyConstraints(edge);
        }
        applyConstraints(tgt);

        carriedSet.push_back(src);
        carriedSet.push_back(edge);
        carriedSet.push_back(tgt);

        if (!canTraverse) {
            earlyMergeTargets.push_back(tgt);
        }
    }

    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Location uloc = _opBuilder.getUnknownLoc();

    for (const VariableDependency* mergeTarget : earlyMergeTargets) {
        for (const DependencyEdge* inEdge : mergeTarget->incoming()) {
            const bool closesACycle = inEdge->isMetaEdge() && !mergesTaken.contains(inEdge);
            if (!closesACycle) {
                continue;
            }

            const mlir::Value copy = findVarOrThrow(_part._varMap, inEdge->src());
            const mlir::Value target = findVarOrThrow(_part._varMap, mergeTarget);

            auto eq = _opBuilder.create<mlir::db::EqOp>(uloc, boolType, copy, target);
            filterAllColumns(eq.getResult());
        }
    }
}

void DBProgramGenerator::applyConstraints(const VariableDependency* var) {
    const auto registeredNodeColumn = [&]() {
        const auto findIt = _part._varMap.find(var);
        const bool registered = findIt != _part._varMap.end() && !findIt->second.empty();
        bioassert(registered, "Label-constrained node not registered: {}", var->getName());

        return findIt->second.back();
    };

    const auto registeredEdgeTypeColumn = [&]() {
        const auto findIt = _part._edgeTypeMap.find(var);
        bioassert(findIt != _part._edgeTypeMap.end(),
                  "Type-constrained edge without a type column: {}", var->getName());

        return findIt->second;
    };

    const auto applyLabelConstraint = [&](const VariableDependency::LabelNames& labels) {
        filterAllColumns(checkNodeLabels(registeredNodeColumn(), labels, false));
    };

    const auto applyEdgeTypeConstraint = [&](const VariableDependency::EdgeTypeNames& types) {
        filterAllColumns(checkEdgeType(registeredEdgeTypeColumn(), types, false));
    };

    const std::optional<VariableDependency::Constraint>& constraints = var->constraints();
    if (constraints.has_value()) {
        std::visit([&](auto&& constraint) {
            using T = std::decay_t<decltype(constraint)>;
            if constexpr (std::is_same_v<T, VariableDependency::LabelNames>) {
                applyLabelConstraint(constraint);
            } else if constexpr (std::is_same_v<T, VariableDependency::EdgeTypeNames>) {
                applyEdgeTypeConstraint(constraint);
            } else {
                // sizeof(T) == 0 (never true) keeps the assert dependent on T so it only
                // fires if this branch is ever instantiated; a bare static_assert(false)
                // is diagnosed eagerly by pre-P2593 compilers even when discarded.
                static_assert(sizeof(T) == 0, "Unhandled constraint type.");
            }
        }, *constraints);
    }

    for (const LabelExpr* labelExpr : var->labelExprConstraints()) {
        filterAllColumns(checkNodeLabelExpr(registeredNodeColumn(), labelExpr, false));
    }

    for (const LabelExpr* typeExpr : var->edgeTypeExprConstraints()) {
        filterAllColumns(checkEdgeTypeExpr(registeredEdgeTypeColumn(), typeExpr, false));
    }
}

void DBProgramGenerator::filterNullNode(const VariableDependency* var) {
    const VarDecl* decl = var->getDecl();
    const bool isANode = decl && decl->getType() == EvaluatedType::NodePattern;
    const bool isNamedByAPattern = var->isNamedByAPattern();

    if (!isANode || !isNamedByAPattern) {
        return;
    }

    // A label check and a hop crossing an edge already reject a null. A negated label holds
    // on one, and a hop of zero edges pairs a null with itself. A hop's length sits on the
    // link from its source to its edge variable, which its target reaches through that variable
    const bool checksALabel = var->constraints().has_value();

    const auto mayCrossNoEdge = [](const DependencyEdge* link) {
        const EdgeMetadata data = link->data();
        return data.isQuantified() && data.getMinHops() == 0;
    };

    bool crossesAnEdge = false;
    for (const DependencyEdge* edge : var->edges()) {
        const VariableDependency* hop = edge->src() == var ? edge->tgt() : edge->src();

        bool hopMayCrossNoEdge = mayCrossNoEdge(edge);
        for (const DependencyEdge* hopLink : hop->edges()) {
            hopMayCrossNoEdge = hopMayCrossNoEdge || mayCrossNoEdge(hopLink);
        }

        crossesAnEdge = crossesAnEdge || !hopMayCrossNoEdge;
    }

    if (checksALabel || crossesAnEdge) {
        return;
    }

    const mlir::db::ColumnType boolType = allocColumnType(mlir::storage::BoolType::get(_mlirCtxt));
    const mlir::Value column = findVarOrThrow(_part._varMap, var);

    filterAllColumns(_opBuilder.create<mlir::db::IsNotNullOp>(_opBuilder.getUnknownLoc(), boolType, column).getResult());
}
