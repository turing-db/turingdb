#include "DBPasses.h"

#include <algorithm>
#include <memory>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "views/GraphView.h"

#include "DBOps.h"
#include "DBCrossProducts.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBLineageClimbs.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"
#include "DBPassWorklist.h"
#include "DBRowEstimator.h"
#include "DBWrittenOrder.h"

namespace mlir::db {

#define GEN_PASS_DEF_FUSESCANBYLABEL
#define GEN_PASS_DEF_FUSESCANBYNODEIDS
#define GEN_PASS_DEF_FUSESCANBYPROPERTYVALUE
#define GEN_PASS_DEF_FUSESCANEDGES
#define GEN_PASS_DEF_FUSEEDGESBYTYPE
#define GEN_PASS_DEF_FUSESCANEDGESBYTYPE
#define GEN_PASS_DEF_FUSESCANOUTEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANINEDGESBYLABEL
#define GEN_PASS_DEF_FUSESCANEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_FUSEEDGESBYENDPOINTLABEL
#define GEN_PASS_DEF_COUNTFROMMETADATA
#include "DBPasses.h.inc"

namespace {

struct ScanSource {
    Operation* _op {nullptr};
    ArrayAttr _labels;
    Value _column;
};

// Any carried column beyond the scan's own (a property read before the WHERE, say) has no
// filtered counterpart in a source op to be rewired to, so such a filter has to stay.
bool matchSoleScanSource(FilterOp filter, ScanSource& source) {
    const Operation::operand_range columns = filter.getColumnsToFilter();
    if (columns.size() != 1) {
        return false;
    }

    source._column = columns.front();
    source._op = source._column.getDefiningOp();
    if (!source._op || !isa<ScanNodes, ScanNodesByLabel>(source._op)) {
        return false;
    }

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(source._op)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

// A test of labels every node a scan makes has to carry, in the mask of a filter nothing but
// which reads the scan: the scan reads the nodes carrying them alone instead
struct LabelScanChain {
    FilterOp _filter;
    Value _conjunct;
    Operation* _scan {nullptr};
    ArrayAttr _labels;
};

bool matchLabelScanConjunct(Value conjunct, Value scanned, LabelScanChain& chain) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    const ArrayAttr conjunction = check ? check.getConjunction() : ArrayAttr();
    if (!conjunction) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet || labelSet.getInputNodes() != scanned) {
        return false;
    }

    chain._conjunct = conjunct;
    chain._labels = conjunction;

    return true;
}

bool matchLabelScanChain(FilterOp filter, LabelScanChain& chain) {
    Operation* const scan = filteredSource(filter);
    if (!isa_and_nonnull<ScanNodes, ScanNodesByLabel>(scan) || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    chain._filter = filter;
    chain._scan = scan;

    const Value scanned = scan->getResult(0);

    return matchAConjunct(filter, [scanned, &chain](Value conjunct) {
        return matchLabelScanConjunct(conjunct, scanned, chain);
    });
}

void fuseScanByLabel(LabelScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._scan;

    llvm::SmallVector<Attribute, 4> labels;
    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        const ArrayAttr scanLabels = scanByLabel.getLabels();
        labels.append(scanLabels.begin(), scanLabels.end());
    }

    for (const Attribute label : chain._labels) {
        if (!llvm::is_contained(labels, label)) {
            labels.push_back(label);
        }
    }

    rewriter.setInsertionPoint(scan);
    ScanNodesByLabel fused = rewriter.create<ScanNodesByLabel>(chain._filter.getLoc(),
                                                               scan->getResult(0).getType(),
                                                               rewriter.getArrayAttr(labels));

    rewriter.replaceOp(scan, fused.getResult());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByLabel : public impl::FuseScanByLabelBase<FuseScanByLabel> {
    void runOnOperation() override {
        runFilterWorklist<LabelScanChain>(getOperation(), matchLabelScanChain, fuseScanByLabel);
    }
};

struct NodeIDScanChain {
    FilterOp _filter;
    Value _conjunct;
    ScanSource _source;
    EntityIDList _nodeIDs;
};

// A disjunction over the scanned node read after the hops and filters of the pattern
// rooted at the scan: it restricts the scan itself
bool matchPatternScanSource(FilterOp filter, Value column, LineageClimbs& climbs, OpAnswers& reaching, ScanSource& source) {
    const Value anchor = climbs.anchorOf(column);
    Operation* const scan = anchor ? anchor.getDefiningOp() : nullptr;
    const bool isNodeScan = isa_and_nonnull<ScanNodes, ScanNodesByLabel>(scan);
    if (!isNodeScan || scan->getBlock() != filter->getBlock()) {
        return false;
    }

    const bool reachesTheFilter = answerOnce(reaching, scan, [scan, &filter]() {
        return rowsReachTheFilter(scan, filter, keepsTheSourceRows);
    });

    if (!reachesTheFilter) {
        return false;
    }

    source._op = scan;
    source._column = column;
    source._labels = ArrayAttr();

    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        source._labels = scanByLabel.getLabels();
    }

    return true;
}

bool matchNodeIDConjunct(FilterOp filter, Value conjunct, LineageClimbs& climbs, OpAnswers& reaching, NodeIDScanChain& chain) {
    chain._nodeIDs._literals.clear();
    chain._nodeIDs._bound.clear();
    chain._filter = filter;
    chain._conjunct = conjunct;

    const Value column = nodeDisjunctionColumn(conjunct);
    if (column) {
        return collectIDDisjunction(conjunct, column, chain._nodeIDs)
            && matchPatternScanSource(filter, column, climbs, reaching, chain._source);
    }

    // A disjunction comparing no node, every disjunct of it never true, keeps no row of the
    // scan the filter reads
    const bool listsNoNode = matchSoleScanSource(filter, chain._source)
                          && collectIDDisjunction(conjunct, chain._source._column, chain._nodeIDs);

    return listsNoNode && readsOnlyIntoTheFilter(chain._source._op, filter);
}

bool matchNodeIDScanChain(FilterOp filter, LineageClimbs& climbs, NodeIDScanChain& chain) {
    OpAnswers reaching;

    return matchAConjunct(filter, [&filter, &climbs, &reaching, &chain](Value conjunct) {
        return matchNodeIDConjunct(filter, conjunct, climbs, reaching, chain);
    });
}

// The listed nodes start the pattern in the scan's place, and every row it builds from them
// holds the disjunction
void fuseScanByNodeIDs(NodeIDScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._source._op;
    const mlir::Location loc = chain._filter.getLoc();

    rewriter.setInsertionPoint(scan);
    ConstScanNodes constScan = rewriter.create<ConstScanNodes>(loc,
                                                               scan->getResult(0).getType(),
                                                               chain._nodeIDs._literals,
                                                               chain._nodeIDs._bound);
    computeBoundValuesAhead(constScan);

    llvm::SmallVector<Value> fused {constScan.getResult()};
    if (chain._source._labels) {
        keepLabelledRows(fused, chain._source._labels, loc, rewriter);
    }

    rewriter.replaceOp(scan, fused.front());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByNodeIDs : public impl::FuseScanByNodeIDsBase<FuseScanByNodeIDs> {
    void runOnOperation() override {
        LineageClimbs climbs;

        const auto matchChain = [&climbs](FilterOp filter, NodeIDScanChain& chain) {
            return matchNodeIDScanChain(filter, climbs, chain);
        };

        const auto fuseChain = [&climbs](NodeIDScanChain& chain, mlir::RewriterBase& rewriter) {
            climbs.clear();
            fuseScanByNodeIDs(chain, rewriter);
        };

        runFilterWorklist<NodeIDScanChain>(getOperation(), matchChain, fuseChain);
    }
};

// A hop over a whole-graph node scan walks every node's edges, which is the whole edge
// set - what a scan_edges produces on its own.
bool matchWholeEdgeSetHop(Operation* hop, ScanNodes& scan) {
    // get_edges walks both directions, so over every node it reports each edge twice, once
    // from each endpoint, and the by-type hops keep one type. Only the directed untyped
    // hops read the edge set a scan_edges produces.
    if (!isa<GetOutEdges, GetInEdges>(hop)) {
        return false;
    }

    // Operand 0 is the input node column and anything after it is a carry set, row-aligned
    // with the scan and with no counterpart in a scan_edges to be rewired to.
    if (hop->getNumOperands() != 1) {
        return false;
    }

    scan = hop->getOperand(0).getDefiningOp<ScanNodes>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

void fuseScanEdges(Operation* hop, ScanNodes scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // Both directed hops fill srcids and tgtids with the edge's own source and target -
    // the direction only decided which side was walked from - so the hop's four results
    // map one-for-one onto the edge scan's, which are declared in the same order.
    ScanEdges edgeScan = builder.create<ScanEdges>(hop->getLoc(),
                                                   hop->getResult(0).getType(),
                                                   hop->getResult(1).getType(),
                                                   hop->getResult(2).getType(),
                                                   hop->getResult(3).getType());

    hop->replaceAllUsesWith(edgeScan.getOperation());
    hop->erase();
    scan.erase();
}

struct FuseScanEdges : public impl::FuseScanEdgesBase<FuseScanEdges> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
        llvm::SmallVector<Operation*> hops;
        root->walk([&](Operation* op) {
            ScanNodes scan;
            if (matchWholeEdgeSetHop(op, scan)) {
                hops.push_back(op);
            }
        });

        mlir::OpBuilder builder(&getContext());
        for (Operation* const hop : hops) {
            ScanNodes scan;
            if (!matchWholeEdgeSetHop(hop, scan)) {
                continue;
            }

            fuseScanEdges(hop, scan, builder);
        }
    }
};

// A directed hop whose rows are then cut down to the types the pattern named: the by-type
// hop spelled the long way, since the walk itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedHop {
    FilterOp _filter;
    Value _conjunct;
    Operation* _hop {nullptr};
    ArrayAttr _edgeTypes;
};

bool matchTypedHopConjunct(Value conjunct, Operation* hop, TypedHop& typedHop) {
    constexpr size_t etypesResultIndex = 2;
    CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check || check.getEdgeTypeIds() != hop->getResult(etypesResultIndex)) {
        return false;
    }

    // The hop walks the disjunction itself, so any number of required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    typedHop._conjunct = conjunct;
    typedHop._edgeTypes = edgeTypes;

    return true;
}

bool matchTypedHop(FilterOp filter, TypedHop& typedHop) {
    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place. And nothing outside the filter may read the hop,
    // or that reader would go on seeing the rows the type turns away.
    Operation* const hop = filteredSource(filter);
    if (!isa_and_nonnull<GetOutEdges, GetInEdges>(hop) || !readsOnlyIntoTheFilter(hop, filter)) {
        return false;
    }

    typedHop._filter = filter;
    typedHop._hop = hop;

    return matchAConjunct(filter, [hop, &typedHop](Value conjunct) {
        return matchTypedHopConjunct(conjunct, hop, typedHop);
    });
}

template <typename ByTypeOp>
Operation* createByTypeHop(Operation* hop, ArrayAttr edgeTypes, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByTypeOp byTypeHop = builder.create<ByTypeOp>(hop->getLoc(),
                                                  results[0].getType(),
                                                  results[1].getType(),
                                                  results[2].getType(),
                                                  results[3].getType(),
                                                  results.drop_front(hopFixedResultCount).getTypes(),
                                                  hop->getOperand(0),
                                                  edgeTypes,
                                                  hop->getOperands().drop_front());

    return byTypeHop.getOperation();
}

void fuseEdgesByType(TypedHop& typedHop, mlir::RewriterBase& rewriter) {
    Operation* const hop = typedHop._hop;

    rewriter.setInsertionPoint(hop);

    // A by-type hop declares the same four fixed results and the same carry set behind them,
    // so the plain hop's results map onto it one for one.
    Operation* const byTypeHop = isa<GetOutEdges>(hop)
                                     ? createByTypeHop<GetOutEdgesByType>(hop, typedHop._edgeTypes, rewriter)
                                     : createByTypeHop<GetInEdgesByType>(hop, typedHop._edgeTypes, rewriter);

    rewriter.replaceOp(hop, byTypeHop->getResults());
    dropConjunct(typedHop._filter, typedHop._conjunct, rewriter);
}

struct FuseEdgesByType : public impl::FuseEdgesByTypeBase<FuseEdgesByType> {
    void runOnOperation() override {
        runFilterWorklist<TypedHop>(getOperation(), matchTypedHop, fuseEdgesByType);
    }
};

// An edge scan whose rows are then cut down to the types the pattern named: the by-type
// scan spelled the long way, since the scan itself can keep the edges of those types and
// never build the rows the filter goes on to drop.
struct TypedEdgeScan {
    FilterOp _filter;
    Value _conjunct;
    ScanEdges _scan;
    ArrayAttr _edgeTypes;
};

bool matchTypedEdgeScanConjunct(Value conjunct, ScanEdges scan, TypedEdgeScan& typedScan) {
    CheckEdgeTypeConstraint check = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();
    if (!check || check.getEdgeTypeIds() != scan.getEtypes()) {
        return false;
    }

    // As in matchTypedHop: the scan walks the disjunction itself, so any number of
    // required types fuses.
    const ArrayAttr edgeTypes = check.getEdgeTypes();
    if (edgeTypes.empty()) {
        return false;
    }

    typedScan._conjunct = conjunct;
    typedScan._edgeTypes = edgeTypes;

    return true;
}

bool matchTypedEdgeScan(FilterOp filter, TypedEdgeScan& typedScan) {
    ScanEdges scan = dyn_cast_or_null<ScanEdges>(filteredSource(filter));
    if (!scan || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    typedScan._filter = filter;
    typedScan._scan = scan;

    return matchAConjunct(filter, [scan, &typedScan](Value conjunct) {
        return matchTypedEdgeScanConjunct(conjunct, scan, typedScan);
    });
}

void fuseScanEdgesByType(TypedEdgeScan& typedScan, mlir::RewriterBase& rewriter) {
    ScanEdges scan = typedScan._scan;

    rewriter.setInsertionPoint(scan);

    // A by-type scan declares the same four results in the same order, so the plain scan's
    // map onto it one for one.
    ScanEdgesByType byTypeScan = rewriter.create<ScanEdgesByType>(scan.getLoc(),
                                                                  scan.getSrcids().getType(),
                                                                  scan.getEids().getType(),
                                                                  scan.getEtypes().getType(),
                                                                  scan.getTgtids().getType(),
                                                                  typedScan._edgeTypes);

    rewriter.replaceOp(scan, byTypeScan->getResults());
    dropConjunct(typedScan._filter, typedScan._conjunct, rewriter);
}

struct FuseScanEdgesByType : public impl::FuseScanEdgesByTypeBase<FuseScanEdgesByType> {
    void runOnOperation() override {
        runFilterWorklist<TypedEdgeScan>(getOperation(), matchTypedEdgeScan, fuseScanEdgesByType);
    }
};

// A directed hop over a by-label node scan walks the edges hanging off every node carrying
// those labels, which is what the matching by-label edge scan reads off the edge index -
// without building the node column the hop walks from.
template <typename HopOp>
bool matchLabelledEdgeScan(HopOp hop, ScanNodesByLabel& scan) {
    // The carry set is row-aligned with the scan and the fused form has nothing of its own
    // to hand back in its place.
    if (!hop.getColumnsToFilter().empty()) {
        return false;
    }

    scan = hop.getInputNodes().template getDefiningOp<ScanNodesByLabel>();
    if (!scan) {
        return false;
    }

    // The fused form drops the node column, so a second reader of it keeps the scan alive.
    return scan.getResult().hasOneUse();
}

template <typename ScanOp, typename HopOp>
void fuseScanEdgesByLabel(HopOp hop, ScanNodesByLabel scan, mlir::OpBuilder& builder) {
    builder.setInsertionPoint(hop);

    // The fused scan declares the same four results in the same order the hop declares its
    // fixed ones, so they map one for one. Both hold the edge as the graph stores it, so
    // the direction changes which edges are produced, not which column holds what.
    ScanOp edgeScan = builder.create<ScanOp>(hop.getLoc(),
                                             hop.getSrcids().getType(),
                                             hop.getEids().getType(),
                                             hop.getEtypes().getType(),
                                             hop.getTgtids().getType(),
                                             scan.getLabelsAttr());

    Operation* const hopOp = hop.getOperation();
    hopOp->replaceAllUsesWith(edgeScan.getOperation());
    hopOp->erase();
    scan.erase();
}

template <typename ScanOp, typename HopOp>
void runScanEdgesByLabelPass(Operation* root, mlir::OpBuilder& builder) {
    // Collect first: fusing erases the hop and its scan, which would invalidate the walk.
    llvm::SmallVector<HopOp> hops;
    root->walk([&](HopOp hop) {
        ScanNodesByLabel scan;
        if (matchLabelledEdgeScan(hop, scan)) {
            hops.push_back(hop);
        }
    });

    for (HopOp hop : hops) {
        ScanNodesByLabel scan;
        if (!matchLabelledEdgeScan(hop, scan)) {
            continue;
        }

        fuseScanEdgesByLabel<ScanOp>(hop, scan, builder);
    }
}

struct FuseScanOutEdgesByLabel : public impl::FuseScanOutEdgesByLabelBase<FuseScanOutEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanOutEdgesByLabelSrc, GetOutEdges>(getOperation(), builder);
    }
};

struct FuseScanInEdgesByLabel : public impl::FuseScanInEdgesByLabelBase<FuseScanInEdgesByLabel> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());
        runScanEdgesByLabelPass<ScanInEdgesByLabelTgt, GetInEdges>(getOperation(), builder);
    }
};

// The index a scan labelled on both ends walks: the one holding fewer edges under its end's
// labels, the source's on a tie. With no graph to count, the one the scan already walks.
bool indexesByTarget(const EdgeScanLabels& scanLabels, const DBPassContext& context) {
    if (!context._view) {
        return scanLabels._indexedByTarget;
    }

    const ::db::GraphView& view = *context._view;
    const ::db::GraphMetadata& metadata = view.metadata();
    const ::db::CardinalityEstimation estimation(view);

    const size_t sourceEdges = estimateIndexedEdges(scanLabels._source, false, metadata, estimation);
    const size_t targetEdges = estimateIndexedEdges(scanLabels._target, true, metadata, estimation);

    return targetEdges < sourceEdges;
}

// An edge scan whose rows are then cut down to the edges one endpoint of which carries a
// set of labels: the scan reading that endpoint by those labels as well, which never builds
// the rows the filter goes on to drop.
struct EndpointLabelledEdgeScan {
    FilterOp _filter;
    Value _conjunct;
    Operation* _scan {nullptr};
    EdgeScanLabels _scanLabels;
    llvm::SmallVector<Attribute, 4> _endLabels;
    bool _labelledTarget {false};
    bool _endWasLabelled {false};
};

bool matchEndpointLabelledEdgeScanConjunct(Value conjunct, EndpointLabelledEdgeScan& labelledScan) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr checkLabels = check.getConjunction();
    if (!checkLabels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    constexpr size_t SRC_RESULT_INDEX = 0;
    constexpr size_t TGT_RESULT_INDEX = 3;
    Operation* const scan = labelledScan._scan;
    const Value labelledColumn = labelSet.getInputNodes();
    const bool labelledTarget = labelledColumn == scan->getResult(TGT_RESULT_INDEX);
    const bool labelledSource = labelledColumn == scan->getResult(SRC_RESULT_INDEX);
    if (!labelledTarget && !labelledSource) {
        return false;
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    const EdgeScanLabels& scanLabels = labelledScan._scanLabels;
    const ArrayAttr endLabels = labelledTarget ? scanLabels._target : scanLabels._source;

    labelledScan._endLabels.clear();
    if (endLabels) {
        labelledScan._endLabels.assign(endLabels.begin(), endLabels.end());
    }

    for (const Attribute label : checkLabels) {
        if (!llvm::is_contained(labelledScan._endLabels, label)) {
            labelledScan._endLabels.push_back(label);
        }
    }

    labelledScan._conjunct = conjunct;
    labelledScan._labelledTarget = labelledTarget;
    labelledScan._endWasLabelled = endLabels != nullptr;

    return true;
}

bool matchEndpointLabelledEdgeScan(FilterOp filter, EndpointLabelledEdgeScan& labelledScan) {
    Operation* const scan = filteredSource(filter);
    if (!scan || !readEdgeScanLabels(scan, labelledScan._scanLabels) || !readsOnlyIntoTheFilter(scan, filter)) {
        return false;
    }

    labelledScan._filter = filter;
    labelledScan._scan = scan;

    return matchAConjunct(filter, [&labelledScan](Value conjunct) {
        return matchEndpointLabelledEdgeScanConjunct(conjunct, labelledScan);
    });
}

// The by-label edge scan reading the endpoints by these labels. A scan labelled on one end
// walks that end's index; one labelled on both walks the index _indexedByTarget names.
Operation* createLabelledEdgeScan(Operation* scan, const EdgeScanLabels& scanLabels, mlir::RewriterBase& rewriter) {
    const Location loc = scan->getLoc();
    const mlir::TypeRange types = scan->getResultTypes();

    if (!scanLabels._source) {
        return rewriter.create<ScanOutEdgesByLabelTgt>(loc, types, scanLabels._target);
    } else if (!scanLabels._target) {
        return rewriter.create<ScanInEdgesByLabelSrc>(loc, types, scanLabels._source);
    } else if (scanLabels._indexedByTarget) {
        return rewriter.create<ScanInEdgesByLabelSrcTgt>(loc, types, scanLabels._source, scanLabels._target);
    }

    return rewriter.create<ScanOutEdgesByLabelSrcTgt>(loc, types, scanLabels._source, scanLabels._target);
}

void fuseScanEdgesByEndpointLabel(EndpointLabelledEdgeScan& labelledScan,
                                  const DBPassContext& context,
                                  mlir::RewriterBase& rewriter) {
    Operation* const scan = labelledScan._scan;
    const ArrayAttr endLabels = rewriter.getArrayAttr(labelledScan._endLabels);

    EdgeScanLabels& scanLabels = labelledScan._scanLabels;
    if (labelledScan._labelledTarget) {
        scanLabels._target = endLabels;
    } else {
        scanLabels._source = endLabels;
    }

    const bool labelsBothEnds = scanLabels._source && scanLabels._target;
    if (labelsBothEnds) {
        scanLabels._indexedByTarget = indexesByTarget(scanLabels, context);
    }

    if (labelledScan._endWasLabelled && !labelsBothEnds) {
        rewriter.modifyOpInPlace(scan, [scan, endLabels]() {
            scan->setAttr("labels", endLabels);
        });
    } else {
        rewriter.setInsertionPoint(scan);
        Operation* const labelledEdgeScan = createLabelledEdgeScan(scan, scanLabels, rewriter);
        rewriter.replaceOp(scan, labelledEdgeScan->getResults());
    }

    dropConjunct(labelledScan._filter, labelledScan._conjunct, rewriter);
}

struct FuseScanEdgesByEndpointLabel : public impl::FuseScanEdgesByEndpointLabelBase<FuseScanEdgesByEndpointLabel> {
    FuseScanEdgesByEndpointLabel() {}

    FuseScanEdgesByEndpointLabel(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        const DBPassContext& context = *_context;
        const auto fuse = [&context](EndpointLabelledEdgeScan& labelledScan, mlir::RewriterBase& rewriter) {
            fuseScanEdgesByEndpointLabel(labelledScan, context, rewriter);
        };

        runFilterWorklist<EndpointLabelledEdgeScan>(getOperation(), matchEndpointLabelledEdgeScan, fuse);
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

// A hop whose rows are then cut down to the edges the endpoint it reaches carries a set of
// labels on: the by-label hop spelled the long way, since the walk itself can keep those
// edges and never build the rows the filter goes on to drop.
struct EndpointLabelledHop {
    FilterOp _filter;
    Value _conjunct;
    Operation* _hop {nullptr};
    ArrayAttr _labels;
};

bool matchEndpointLabelledHopConjunct(Value conjunct, Operation* hop, EndpointLabelledHop& labelledHop) {
    CheckLabelConstraint check = conjunct.getDefiningOp<CheckLabelConstraint>();
    if (!check) {
        return false;
    }

    const ArrayAttr labels = check.getConjunction();
    if (!labels) {
        return false;
    }

    GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
    if (!labelSet) {
        return false;
    }

    // Only the end the hop reaches can ride onto it. The end it leaves is the input column,
    // which a by-label node scan constrains instead - and there the labels are no longer
    // this hop's to carry.
    constexpr size_t srcResultIndex = 0;
    constexpr size_t tgtResultIndex = 3;
    const size_t reachedResultIndex = isReverseHop(hop) ? srcResultIndex : tgtResultIndex;
    if (labelSet.getInputNodes() != hop->getResult(reachedResultIndex)) {
        return false;
    }

    const bool chainIsPrivate = labelSet.getResult().hasOneUse() && check.getResult().hasOneUse();
    if (!chainIsPrivate) {
        return false;
    }

    labelledHop._conjunct = conjunct;
    labelledHop._labels = labels;

    return true;
}

bool matchEndpointLabelledHop(FilterOp filter, EndpointLabelledHop& labelledHop) {
    // Every column the filter cuts has to be one the hop bound, or the fused hop has nothing
    // of its own to hand back in its place. And nothing outside the filter may read the hop,
    // or that reader would go on seeing the rows the labels turn away.
    Operation* const hop = filteredSource(filter);
    const bool fusesLabels = isa_and_nonnull<GetOutEdges, GetInEdges, GetOutEdgesByType, GetInEdgesByType>(hop);
    if (!fusesLabels || !readsOnlyIntoTheFilter(hop, filter)) {
        return false;
    }

    labelledHop._filter = filter;
    labelledHop._hop = hop;

    return matchAConjunct(filter, [hop, &labelledHop](Value conjunct) {
        return matchEndpointLabelledHopConjunct(conjunct, hop, labelledHop);
    });
}

template <typename ByLabelOp>
Operation* createByLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();

    ByLabelOp byLabelHop = builder.create<ByLabelOp>(hop->getLoc(),
                                                     results[0].getType(),
                                                     results[1].getType(),
                                                     results[2].getType(),
                                                     results[3].getType(),
                                                     results.drop_front(hopFixedResultCount).getTypes(),
                                                     hop->getOperand(0),
                                                     labels,
                                                     hop->getOperands().drop_front());

    return byLabelHop.getOperation();
}

template <typename ByTypeAndLabelOp>
Operation* createByTypeAndLabelHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    const Operation::result_range results = hop->getResults();
    const ArrayAttr edgeTypes = hop->getAttrOfType<ArrayAttr>("edge_types");

    ByTypeAndLabelOp byTypeAndLabelHop = builder.create<ByTypeAndLabelOp>(hop->getLoc(),
                                                                          results[0].getType(),
                                                                          results[1].getType(),
                                                                          results[2].getType(),
                                                                          results[3].getType(),
                                                                          results.drop_front(hopFixedResultCount).getTypes(),
                                                                          hop->getOperand(0),
                                                                          edgeTypes,
                                                                          labels,
                                                                          hop->getOperands().drop_front());

    return byTypeAndLabelHop.getOperation();
}

Operation* createEndpointLabelledHop(Operation* hop, ArrayAttr labels, mlir::OpBuilder& builder) {
    if (isa<GetOutEdges>(hop)) {
        return createByLabelHop<GetOutEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetInEdges>(hop)) {
        return createByLabelHop<GetInEdgesByLabel>(hop, labels, builder);
    } else if (isa<GetOutEdgesByType>(hop)) {
        return createByTypeAndLabelHop<GetOutEdgesByTypeAndLabel>(hop, labels, builder);
    } else {
        return createByTypeAndLabelHop<GetInEdgesByTypeAndLabel>(hop, labels, builder);
    }
}

void fuseEdgesByEndpointLabel(EndpointLabelledHop& labelledHop, mlir::RewriterBase& rewriter) {
    Operation* const hop = labelledHop._hop;

    rewriter.setInsertionPoint(hop);

    // A by-label hop declares the same four fixed results and the same carry set behind
    // them, so the plain hop's results map onto it one for one. Which one it becomes is the
    // hop the query wrote: an out-hop reaches its target, an in-hop its source, and a by-type
    // hop keeps its types.
    Operation* const byLabelHop = createEndpointLabelledHop(hop, labelledHop._labels, rewriter);

    rewriter.replaceOp(hop, byLabelHop->getResults());
    dropConjunct(labelledHop._filter, labelledHop._conjunct, rewriter);
}

struct FuseEdgesByEndpointLabel : public impl::FuseEdgesByEndpointLabelBase<FuseEdgesByEndpointLabel> {
    void runOnOperation() override {
        runFilterWorklist<EndpointLabelledHop>(getOperation(), matchEndpointLabelledHop, fuseEdgesByEndpointLabel);
    }
};

// A property equality the scan answers in place of the filter, which keeps its other conjuncts
// over the rows the scan by value makes
struct PropertyValueScanChain {
    FilterOp _filter;
    Value _conjunct;
    ScanSource _source;
    StringAttr _property;
    TypedAttr _value;
    Value _boundValue;
};

bool matchPropertyValueScanChain(FilterOp filter, PropertyValueScanChain& chain) {
    if (!matchSoleScanSource(filter, chain._source) || !readsOnlyIntoTheFilter(chain._source._op, filter)) {
        return false;
    }

    // The first property equality in the mask wins, not the most selective one: there are
    // no column statistics to choose with, so `n.gender = 'M' AND n.ssn = '...'` fuses the
    // unselective conjunct and leaves the selective one a residual filter.
    return matchAConjunct(filter, [&filter, &chain](Value conjunct) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        PropertyEquality property;
        if (!equality || !matchPropertyEquality(equality, property)) {
            return false;
        }

        if (property._read.getInputNodes() != chain._source._column) {
            return false;
        }

        chain._filter = filter;
        chain._conjunct = conjunct;
        chain._property = property._read.getPropertyAttr();
        chain._value = property._value;
        chain._boundValue = property._boundValue;

        return true;
    });
}

void fuseScanByPropertyValue(PropertyValueScanChain& chain, mlir::RewriterBase& rewriter) {
    Operation* const scan = chain._source._op;

    rewriter.setInsertionPoint(scan);
    ScanNodesByPropertyValue fused = rewriter.create<ScanNodesByPropertyValue>(chain._filter.getLoc(),
                                                                               chain._source._column.getType(),
                                                                               chain._property,
                                                                               chain._value,
                                                                               chain._boundValue,
                                                                               chain._source._labels);
    computeBoundValuesAhead(fused);

    rewriter.replaceOp(scan, fused.getResult());
    dropConjunct(chain._filter, chain._conjunct, rewriter);
}

struct FuseScanByPropertyValue : public impl::FuseScanByPropertyValueBase<FuseScanByPropertyValue> {
    void runOnOperation() override {
        runFilterWorklist<PropertyValueScanChain>(getOperation(), matchPropertyValueScanChain, fuseScanByPropertyValue);
    }
};

// The node scans a column's rows come off, in factor order. A cross product's rows are its
// two factors' together, and a factor is free to be a product of its own, so each is walked
// through the column it yields first. Anything else a column can come off - a filter, a hop,
// a property read - has a row count only the engine can reach, so the match fails.
bool collectScans(Value column, llvm::SmallVectorImpl<Operation*>& scans) {
    llvm::SmallVector<Value> pending {column};
    while (!pending.empty()) {
        Operation* const def = pending.pop_back_val().getDefiningOp();
        if (!def) {
            return false;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            scans.push_back(def);
        } else if (CrossProduct product = dyn_cast<CrossProduct>(def)) {
            const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
            const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
            if (leftColumns.empty() || rightColumns.empty()) {
                return false;
            }

            pending.push_back(rightColumns.front());
            pending.push_back(leftColumns.front());
        } else {
            return false;
        }
    }

    return true;
}

// The one scan a column carries the nodes of. Unlike collectScans this follows the result a
// product was read at: its results are the left factor's yielded columns then the right's,
// so an index past the left's count names a column of the right factor.
Operation* scanBehind(Value column) {
    for (;;) {
        Operation* const def = column.getDefiningOp();
        if (!def) {
            return nullptr;
        }

        if (isa<ScanNodes, ScanNodesByLabel>(def)) {
            return def;
        }

        CrossProduct product = dyn_cast<CrossProduct>(def);
        if (!product) {
            return nullptr;
        }

        const Operation::operand_range leftColumns = factorYieldColumns(product.getLeftFactor());
        const Operation::operand_range rightColumns = factorYieldColumns(product.getRightFactor());
        const size_t resultIndex = cast<OpResult>(column).getResultNumber();
        if (resultIndex >= leftColumns.size() + rightColumns.size()) {
            return nullptr;
        }

        column = resultIndex < leftColumns.size() ? leftColumns[resultIndex]
                                                  : rightColumns[resultIndex - leftColumns.size()];
    }
}

// What a count reads: the scans it tallies, the property whose holders one of them is
// narrowed to, if any, and which scan that is.
struct ScanTally {
    llvm::SmallVector<Operation*> scans;
    StringAttr property;
    size_t propertyScan {0};
};

// The label conjunction db.count_scan_rows spells for one scan: its own label list, or an
// empty one for an unlabelled scan.
Attribute scanLabels(Operation* scan, mlir::OpBuilder& builder) {
    if (ScanNodesByLabel scanByLabel = dyn_cast<ScanNodesByLabel>(scan)) {
        return scanByLabel.getLabelsAttr();
    }

    return builder.getStrArrayAttr({});
}

// Whether this count tallies nothing but whole node scans, and what it reads off them.
// count(DISTINCT x) is a different tally over the same rows, so it is left alone.
bool countsWholeScans(Count count, ScanTally& tally) {
    if (count.getDistinct()) {
        return false;
    }

    Value rows = count.getInput();

    // A property read keeps every row and nulls the ones without the value, so count(*)
    // reads through it to the nodes underneath while a plain count tallies the holders.
    if (GetNodeProperties fetch = rows.getDefiningOp<GetNodeProperties>()) {
        rows = fetch.getInputNodes();
        if (!count.getRows()) {
            tally.property = fetch.getPropertyAttr();
        }
    } else {
        // A plain count over anything else charges the non-null rows, which is the row
        // count only on a column that holds no null - the node IDs a scan emits.
        const ColumnType rowsType = dyn_cast<ColumnType>(rows.getType());
        if (!count.getRows() && !(rowsType && isa<storage::NodeIDType>(rowsType.getType()))) {
            return false;
        }
    }

    if (!collectScans(rows, tally.scans)) {
        return false;
    }

    if (!tally.property) {
        return true;
    }

    // The property was read of one of the scans' columns; that scan is the one its holders
    // narrow, and the others contribute their whole node count.
    Operation* const propertyScan = scanBehind(rows);
    const auto scanIt = std::ranges::find(tally.scans, propertyScan);
    if (scanIt == tally.scans.end()) {
        return false;
    }

    tally.propertyScan = static_cast<size_t>(scanIt - tally.scans.begin());

    return true;
}

void countFromMetadata(Count count, const ScanTally& tally, mlir::OpBuilder& builder) {
    llvm::SmallVector<Attribute> labels;
    for (Operation* const scan : tally.scans) {
        labels.push_back(scanLabels(scan, builder));
    }

    // The first scan is where a property is read from unless the op says otherwise, so the
    // common single-scan form carries no index.
    const mlir::Type indexType = builder.getIntegerType(64, /*isSigned=*/false);
    const IntegerAttr propertyScan = tally.propertyScan > 0 ? IntegerAttr::get(indexType, tally.propertyScan)
                                                            : IntegerAttr();

    builder.setInsertionPoint(count);
    CountScanRows scanRows = builder.create<CountScanRows>(count.getLoc(),
                                                           count.getResult().getType(),
                                                           builder.getArrayAttr(labels),
                                                           tally.property,
                                                           propertyScan);

    Operation* const countOp = count.getOperation();
    const Value countedRows = count.getInput();

    countOp->getResult(0).replaceAllUsesWith(scanRows.getResult());
    countOp->erase();

    // Drop the now-dead chain, consumer to producer, each only if unused; a cross product
    // takes the scans in its factor regions down with it.
    Operation* const countedOp = countedRows.getDefiningOp();
    GetNodeProperties fetch = dyn_cast<GetNodeProperties>(countedOp);
    const Value scanned = fetch ? fetch.getInputNodes() : Value();

    eraseIfUnused(countedOp);
    if (scanned) {
        eraseIfUnused(scanned.getDefiningOp());
    }
}

// Whether the query writes to the graph. The graph's counts leave out the query's own
// writes, so none of its counts can be read off them.
bool writesTheGraph(Operation* root) {
    const WalkResult walked = root->walk([](Operation* op) {
        if (isGraphWrite(op)) {
            return WalkResult::interrupt();
        }

        return WalkResult::advance();
    });

    return walked.wasInterrupted();
}

struct CountFromMetadata : public impl::CountFromMetadataBase<CountFromMetadata> {
    CountFromMetadata() {}

    CountFromMetadata(const DBPassContext* context)
        : _context(context)
    {
    }

    void runOnOperation() override {
        if (_context->_hasPendingWrites || writesTheGraph(getOperation())) {
            return;
        }

        // Collect the counts first, since rewriting erases ops and would invalidate the walk.
        llvm::SmallVector<Count> counts;
        getOperation()->walk([&](Count count) {
            counts.push_back(count);
        });

        mlir::OpBuilder builder(&getContext());
        for (const Count count : counts) {
            ScanTally tally;
            if (!countsWholeScans(count, tally)) {
                continue;
            }

            countFromMetadata(count, tally, builder);
        }
    }

private:
    const DBPassContext* _context {&defaultPassContext};
};

}

std::unique_ptr<Pass> createFuseScanEdgesByEndpointLabel(const DBPassContext* context) {
    return std::make_unique<FuseScanEdgesByEndpointLabel>(context);
}

std::unique_ptr<Pass> createCountFromMetadata(const DBPassContext* context) {
    return std::make_unique<CountFromMetadata>(context);
}

}
