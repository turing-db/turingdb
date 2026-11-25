#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <map>
#include <optional>
#include <string_view>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "CardinalityEstimation.h"
#include "metadata/GraphMetadata.h"
#include "metadata/LabelSet.h"
#include "views/GraphView.h"

#include "DBColumnElement.h"
#include "DBOps.h"
#include "DBFilterUtils.h"
#include "DBKnownLabels.h"
#include "DBLookupPredicates.h"
#include "DBPassUtils.h"

namespace mlir::db {

struct JunctionJoins;
struct SeedPattern;
struct WalkEvent;

// The label set a scan's label names stand for, resolved against the schema, and whether
// the schema holds every one of them.
bool collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels);

// The edges the index keyed by one end's labels holds under these labels, which is what a
// scan of it walks, and none when the schema lacks one of them.
size_t estimateIndexedEdges(ArrayAttr labelNames,
                            bool indexedByTarget,
                            const ::db::GraphMetadata& metadata,
                            const ::db::CardinalityEstimation& estimation);

// System R's share of rows a predicate keeps (Selinger et al., 1979): the graph counts
// nodes per label set and edges per end, but no value per property.
constexpr double RANGE_SELECTIVITY = 1.0 / 3.0;
constexpr double MEMBERSHIP_SELECTIVITY_CEILING = 0.5;

// PostgreSQL's count of distinct values in a column it holds no statistics of
// (DEFAULT_NUM_DISTINCT): as many as the rows, up to this many, and two in a boolean one
constexpr double DEFAULT_DISTINCT_VALUES = 200;
constexpr double BOOLEAN_DISTINCT_VALUES = 2;

double estimateUnwoundRows(Unwind unwind);

using KnownLabels = llvm::DenseMap<Value, ::db::LabelSet>;

// The edges a hop walks as far as the estimate tells them apart: the labels known on the
// node it leaves and on the node it reaches, and the types it keeps, every type until one
// is named
struct HopEdges {
    ::db::LabelSet _near;
    ::db::LabelSet _far;
    llvm::SmallVector<::db::EdgeTypeID, 2> _types;
    bool _isTyped {false};
    bool _leavesSource {true};
    bool _walksBoth {false};
};

// The hop a walk last took at a node, and whether the node is the source of its edges
struct NodeSide {
    Operation* _hop {nullptr};
    bool _isOut {true};
};

// What an estimate walking a pattern's ops knows of its rows: the labels each node carries,
// the edges each hop walked, found from the hop and from the node it reached, and the hop
// the walk last took at each node
struct WalkKnowledge {
    KnownLabels _labels;
    llvm::DenseMap<Operation*, HopEdges> _hops;
    llvm::DenseMap<Value, Operation*> _reachingHops;
    llvm::DenseMap<Value, NodeSide> _lastSides;

    // A result an op hands on holds values of the column its rows come from, so it is
    // known as that column is: a filter keeps the labels of the nodes it reads
    void handOn(Operation* op) {
        ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);
        if (!carrier) {
            return;
        }

        for (const OpResult result : op->getResults()) {
            if (const Value rowSource = carrier.getRowSource(result)) {
                handOn(rowSource, result);
            }
        }
    }

    void handOn(Value rowSource, Value column) {
        const auto labels = _labels.find(rowSource);
        if (labels != _labels.end()) {
            const ::db::LabelSet sourceLabels = labels->second;
            _labels[column] = sourceLabels;
        }

        if (Operation* const reachingHop = _reachingHops.lookup(rowSource)) {
            _reachingHops[column] = reachingHop;
        }

        const auto lastSide = _lastSides.find(rowSource);
        if (lastSide != _lastSides.end()) {
            const NodeSide sourceSide = lastSide->second;
            _lastSides[column] = sourceSide;
        }
    }
};

// What a hop makes and reads per row it reads
struct HopEstimate {
    double _rows {0};
    double _edgesRead {0};
};

// The edge types a list of names stands for, a name the schema lacks standing for no edge
void collectEdgeTypes(ArrayAttr names, const ::db::GraphMetadata& metadata, llvm::SmallVectorImpl<::db::EdgeTypeID>& edgeTypes);

// The hop or edge scan a column of edge types was read from: its own type column, or the
// types of its edge column
Operation* findTypedHop(Value edgeTypes, llvm::function_ref<Value(Value)> nodeOf);

class RowEstimator {
public:
    explicit RowEstimator(const ::db::GraphView& view)
        : _metadata(view.metadata()),
        _estimation(view),
        _nodeCount(static_cast<double>(_estimation.estimateNodeCount(::db::LabelSet {}))),
        _edgeCount(static_cast<double>(_estimation.estimateEdgeCount()))
    {
    }

    double getNodeCount() const { return _nodeCount; }
    double getEdgeCount() const { return _edgeCount; }

    double getLabelledNodes(const ::db::LabelSet& labels) const {
        if (labels.empty()) {
            return _nodeCount;
        }

        return static_cast<double>(_estimation.estimateNodeCount(labels));
    }

    // The nodes a scan reads: those carrying every label @param labelNames names, every node
    // when it names none, whose labels go into @param knowledge for the column @param nodes
    double scanNodes(Value nodes, ArrayAttr labelNames, WalkKnowledge& knowledge) const {
        if (!labelNames) {
            return _nodeCount;
        }

        ::db::LabelSet labels;
        if (!collectScanLabels(labelNames, _metadata, labels)) {
            return 0;
        }

        knowledge._labels[nodes] = labels;

        return getLabelledNodes(labels);
    }

    // The nodes a scan by property value makes: of those scanNodes reads, the ones holding
    // one of the property's distinct values
    double scanNodesByPropertyValue(ScanNodesByPropertyValue scan, WalkKnowledge& knowledge) const {
        const std::optional<ArrayAttr> labels = scan.getLabels();
        const double scanned = scanNodes(scan.getResult(), labels ? *labels : ArrayAttr(), knowledge);

        const TypedAttr value = scan.getValueAttr();
        const Type element = value ? value.getType() : ::db::declaredColumnElement(scan.getBoundValue());

        return scanned * getEqualityShare(getPropertyDistinctValues(element, scanned));
    }

    // The rows an edge scan makes: the edges of the types @param typeNames names, every type
    // when it names none, from the nodes carrying every label @param sourceNames names to
    // those carrying every label @param targetNames names, an end with no names being any
    // node. What it walked goes into @param knowledge, as a hop out of its sources would.
    double scanEdges(Operation* scan,
                     ArrayAttr sourceNames,
                     ArrayAttr targetNames,
                     ArrayAttr typeNames,
                     WalkKnowledge& knowledge) const {
        HopEdges edges;
        const bool resolvesSource = !sourceNames || collectScanLabels(sourceNames, _metadata, edges._near);
        const bool resolvesTarget = !targetNames || collectScanLabels(targetNames, _metadata, edges._far);
        if (typeNames) {
            narrowTypes(typeNames, edges);
        }

        const Value sources = scan->getResult(0);
        const Value targets = scan->getResult(EDGE_TARGETS_RESULT);

        knowledge._labels[sources] = edges._near;
        knowledge._labels[targets] = edges._far;
        knowledge._hops[scan] = edges;
        knowledge._reachingHops[targets] = scan;
        knowledge._lastSides[sources] = NodeSide {._hop = scan, ._isOut = true};
        knowledge._lastSides[targets] = NodeSide {._hop = scan, ._isOut = false};

        if (!resolvesSource || !resolvesTarget) {
            return 0;
        }

        return countHopEdges(edges);
    }

    // The rows a hop makes and the edges it reads per row it reads: the edges it walks per
    // node carrying the labels known on @param from, of its own types or else of @param
    // keptTypes, which a filter of its type column keeps, and every edge on that side of the
    // node, since a node's edges of one type are scattered through all of them. A node the
    // walk took a hop at before has the edges of the nodes that hop leads to, not the
    // average. What it walked goes into @param knowledge.
    HopEstimate walkHop(Operation* hop,
                        bool walksBackward,
                        Value from,
                        Value to,
                        ArrayAttr keptTypes,
                        WalkKnowledge& knowledge) const {
        HopEdges edges;
        edges._near = knowledge._labels.lookup(from);
        edges._far = knowledge._labels.lookup(to);
        edges._leavesSource = isReverseHop(hop) == walksBackward;
        edges._walksBoth = isa<GetEdges>(hop);

        bool reachesLabelledNodes = true;
        if (isEdgeHop(hop)) {
            const ArrayAttr hopTypes = hop->getAttrOfType<ArrayAttr>("edge_types");
            const ArrayAttr types = hopTypes ? hopTypes : keptTypes;
            if (types) {
                narrowTypes(types, edges);
            }

            const ArrayAttr reachedLabels = hop->getAttrOfType<ArrayAttr>("labels");
            if (reachedLabels && !walksBackward) {
                reachesLabelledNodes = collectScanLabels(reachedLabels, _metadata, edges._far);
            }
        }

        HopEdges read;
        read._near = edges._near;
        read._leavesSource = edges._leavesSource;
        read._walksBoth = edges._walksBoth;

        const bool relatesToEarlierHops = isEdgeHop(hop) && !edges._walksBoth;

        double rowsLift = 1;
        double readLift = 1;
        const auto previous = knowledge._lastSides.find(from);
        if (relatesToEarlierHops && previous != knowledge._lastSides.end()) {
            const NodeSide previousSide = previous->second;
            const HopEdges& previousEdges = knowledge._hops.find(previousSide._hop)->second;
            const ::db::DegreeSelection previousDegree = selectionOf(previousEdges, previousSide._isOut);

            rowsLift = getDegreeLift(edges._near, previousDegree, selectionOf(edges, edges._leavesSource));
            readLift = getDegreeLift(edges._near, previousDegree, selectionOf(read, read._leavesSource));
        }

        knowledge._labels[to] = edges._far;
        knowledge._hops[hop] = edges;
        knowledge._reachingHops[to] = hop;

        if (relatesToEarlierHops) {
            knowledge._lastSides[from] = NodeSide {._hop = hop, ._isOut = edges._leavesSource};
            knowledge._lastSides[to] = NodeSide {._hop = hop, ._isOut = !edges._leavesSource};
        } else {
            knowledge._lastSides.erase(from);
            knowledge._lastSides.erase(to);
        }

        const double nodes = getLabelledNodes(edges._near);
        if (nodes == 0) {
            return HopEstimate {};
        }

        HopEstimate estimate;
        estimate._edgesRead = readLift * countHopEdges(read) / nodes;
        if (reachesLabelledNodes) {
            estimate._rows = rowsLift * countHopEdges(edges) / nodes;
        }

        return estimate;
    }

    // The share of rows a label check on @param node keeps, which then adds the labels every
    // alternative asks for to what @param knowledge knows of the node
    double keepLabelled(ArrayAttr alternatives, Value node, WalkKnowledge& knowledge) const {
        const double kept = shareLabelled(alternatives, node, knowledge);

        ::db::LabelSet shared;
        addSharedLabels(alternatives, shared);
        knowledge._labels[node].merge(shared);

        const auto reaching = knowledge._reachingHops.find(node);
        if (reaching != knowledge._reachingHops.end()) {
            knowledge._hops[reaching->second]._far.merge(shared);
        }

        return kept;
    }

    // @param nodeOf names the node a column holds, whose labels @param knowledge keeps
    double getSelectivity(Value mask, llvm::function_ref<Value(Value)> nodeOf, WalkKnowledge& knowledge) const {
        llvm::SmallVector<Value, 4> conjuncts;
        collectConjuncts(mask, conjuncts);

        double kept = 1;
        for (const Value conjunct : conjuncts) {
            const LabelCheck check = readLabelCheck(conjunct);
            CheckEdgeTypeConstraint typeCheck = conjunct.getDefiningOp<CheckEdgeTypeConstraint>();

            if (check._alternatives) {
                kept *= keepLabelled(check._alternatives, nodeOf(check._nodes), knowledge);
            } else if (typeCheck) {
                kept *= keepTyped(typeCheck, nodeOf, knowledge);
            } else {
                kept *= getPredicateSelectivity(conjunct, nodeOf, knowledge);
            }
        }

        return kept;
    }

    // A flat OR or AND chain is as long as the query, so the walk keeps its own stack
    double getPredicateSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        llvm::DenseMap<Value, double> kept;
        llvm::SmallVector<Value> pending {predicate};

        while (!pending.empty()) {
            const Value current = pending.back();
            if (kept.count(current)) {
                pending.pop_back();
                continue;
            }

            Operation* const def = current.getDefiningOp();
            if (!isa_and_nonnull<AndOp, OrOp, XorOp, NotOp>(def)) {
                kept[current] = getLeafSelectivity(current, nodeOf, knowledge);
                pending.pop_back();
                continue;
            }

            bool operandsPending = false;
            for (const Value operand : def->getOperands()) {
                if (!kept.count(operand)) {
                    pending.push_back(operand);
                    operandsPending = true;
                }
            }

            if (operandsPending) {
                continue;
            }

            const double first = kept.lookup(def->getOperand(0));
            const double second = def->getNumOperands() > 1 ? kept.lookup(def->getOperand(1)) : 0;

            if (isa<AndOp>(def)) {
                kept[current] = first * second;
            } else if (isa<OrOp>(def)) {
                kept[current] = first + second - first * second;
            } else if (isa<XorOp>(def)) {
                kept[current] = first + second - 2 * first * second;
            } else {
                kept[current] = 1 - first;
            }

            pending.pop_back();
        }

        return kept.lookup(predicate);
    }

private:
    // The edge counts and degree products asked for, read off the graph once each: the walks
    // the estimate compares ask for the same ones over and over
    using StatisticKey = llvm::SmallVector<uint64_t, 16>;

    static constexpr uint64_t EDGE_COUNT_KEY = 0;
    static constexpr uint64_t DEGREE_PRODUCTS_KEY = 1;

    const ::db::GraphMetadata& _metadata;
    ::db::CardinalityEstimation _estimation;
    double _nodeCount {0};
    double _edgeCount {0};
    mutable std::map<StatisticKey, double> _statistics;

    struct LabelCheck {
        ArrayAttr _alternatives;
        Value _nodes;
    };

    static LabelCheck readLabelCheck(Value predicate) {
        CheckLabelConstraint check = predicate.getDefiningOp<CheckLabelConstraint>();
        if (!check) {
            return {};
        }

        GetNodeLabelSet labelSet = check.getLabelsetIds().getDefiningOp<GetNodeLabelSet>();
        if (!labelSet) {
            return {};
        }

        return LabelCheck {._alternatives = check.getAlternatives(), ._nodes = labelSet.getInputNodes()};
    }

    static void appendLabels(const ::db::LabelSet& labels, StatisticKey& key) {
        for (const uint64_t word : labels.integers()) {
            key.push_back(word);
        }
    }

    static void appendDegree(const ::db::DegreeSelection& degree, StatisticKey& key) {
        key.push_back(degree._isOut ? 1 : 0);
        key.push_back(degree._isEveryType ? 1 : 0);
        key.push_back(degree._edgeTypes.size());
        for (const ::db::EdgeTypeID edgeType : degree._edgeTypes) {
            key.push_back(edgeType.getValue());
        }
    }

    double countEdges(const ::db::LabelSet& source, const HopEdges& edges, const ::db::LabelSet& target) const {
        StatisticKey key {EDGE_COUNT_KEY};
        appendLabels(source, key);
        appendLabels(target, key);
        appendDegree(selectionOf(edges, true), key);

        const auto [known, isNew] = _statistics.try_emplace(key, 0);
        if (!isNew) {
            return known->second;
        }

        double count = 0;
        if (!edges._isTyped) {
            count = static_cast<double>(_estimation.estimateEdgeCount(source, target));
        } else {
            for (const ::db::EdgeTypeID edgeType : edges._types) {
                count += static_cast<double>(_estimation.estimateEdgeCount(source, edgeType, target));
            }
        }

        known->second = count;

        return count;
    }

    double sumDegreeProducts(const ::db::LabelSet& labels,
                             const ::db::DegreeSelection& first,
                             const ::db::DegreeSelection& second) const {
        StatisticKey key {DEGREE_PRODUCTS_KEY};
        appendLabels(labels, key);
        appendDegree(first, key);
        appendDegree(second, key);

        const auto [known, isNew] = _statistics.try_emplace(key, 0);
        if (isNew) {
            known->second = static_cast<double>(_estimation.estimateDegreeProducts(labels, first, second));
        }

        return known->second;
    }

    double countHopEdges(const HopEdges& edges) const {
        const ::db::LabelSet& source = edges._leavesSource ? edges._near : edges._far;
        const ::db::LabelSet& target = edges._leavesSource ? edges._far : edges._near;

        double count = countEdges(source, edges, target);
        if (edges._walksBoth) {
            count += countEdges(target, edges, source);
        }

        return count;
    }

    static ::db::DegreeSelection selectionOf(const HopEdges& edges, bool isOut) {
        return ::db::DegreeSelection {._edgeTypes = edges._types, ._isEveryType = !edges._isTyped, ._isOut = isOut};
    }

    // The edges @param degree selects on the nodes carrying @param labels
    double countSelectedEdges(const ::db::LabelSet& labels, const ::db::DegreeSelection& degree) const {
        HopEdges edges;
        edges._near = labels;
        edges._types.assign(degree._edgeTypes.begin(), degree._edgeTypes.end());
        edges._isTyped = !degree._isEveryType;
        edges._leavesSource = degree._isOut;

        return countHopEdges(edges);
    }

    // How many times the label set's average of the edges @param next selects a node carrying
    // @param labels has when the walk took the edges @param previous selects there before:
    // a walk comes to a node with many edges more often than to one with few
    double getDegreeLift(const ::db::LabelSet& labels,
                         const ::db::DegreeSelection& previous,
                         const ::db::DegreeSelection& next) const {
        const double nodes = getLabelledNodes(labels);
        const double previousEdges = countSelectedEdges(labels, previous);
        const double nextEdges = countSelectedEdges(labels, next);
        if (nodes == 0 || previousEdges == 0 || nextEdges == 0) {
            return 1;
        }

        const double products = sumDegreeProducts(labels, previous, next);

        return (products / previousEdges) / (nextEdges / nodes);
    }

    // Keeps the types of @param edges the names ask for, every one of them when the edges
    // were of any type
    void narrowTypes(ArrayAttr typeNames, HopEdges& edges) const {
        llvm::SmallVector<::db::EdgeTypeID, 2> named;
        collectEdgeTypes(typeNames, _metadata, named);

        if (edges._isTyped) {
            llvm::erase_if(edges._types, [&named](::db::EdgeTypeID edgeType) {
                return !llvm::is_contained(named, edgeType);
            });
        } else {
            edges._types.assign(named.begin(), named.end());
            edges._isTyped = true;
        }
    }

    void addSharedLabels(ArrayAttr alternatives, ::db::LabelSet& labels) const {
        llvm::DenseSet<Attribute> sharedNames;
        addSharedLabelNames(alternatives, sharedNames);

        const ::db::LabelMap& labelMap = _metadata.labels();
        for (const Attribute name : sharedNames) {
            const StringAttr nameAttr = dyn_cast<StringAttr>(name);
            if (!nameAttr) {
                continue;
            }

            const llvm::StringRef nameRef = nameAttr.getValue();
            const std::optional<::db::LabelID> label = labelMap.get(std::string_view(nameRef.data(), nameRef.size()));
            if (label) {
                labels.set(*label);
            }
        }
    }

    // The share of nodes carrying @param known that carry the labels of one alternative
    double shareLabelled(ArrayAttr alternatives, const ::db::LabelSet& known) const {
        const double knownNodes = getLabelledNodes(known);
        if (knownNodes == 0) {
            return 0;
        }

        double kept = 0;
        for (const Attribute alternative : alternatives) {
            ::db::LabelSet labels;
            if (collectScanLabels(cast<ArrayAttr>(alternative), _metadata, labels)) {
                labels.merge(known);
                kept += getLabelledNodes(labels);
            }
        }

        return std::min(1.0, kept / knownNodes);
    }

    // A node a hop reached is checked among the edges that hop walked, so a type that only
    // ever reaches nodes of a label keeps all of them; any other among the nodes it is
    // known to carry the labels of
    double shareLabelled(ArrayAttr alternatives, Value node, const WalkKnowledge& knowledge) const {
        const auto reaching = knowledge._reachingHops.find(node);
        if (reaching == knowledge._reachingHops.end()) {
            return shareLabelled(alternatives, knowledge._labels.lookup(node));
        }

        const HopEdges& edges = knowledge._hops.find(reaching->second)->second;
        const double walked = countHopEdges(edges);
        if (walked == 0) {
            return 0;
        }

        double kept = 0;
        for (const Attribute alternative : alternatives) {
            HopEdges labelled = edges;
            if (collectScanLabels(cast<ArrayAttr>(alternative), _metadata, labelled._far)) {
                kept += countHopEdges(labelled);
            }
        }

        return std::min(1.0, kept / walked);
    }

    // The share of rows a type check keeps: of the edges its hop walked, or of the graph's
    // when no hop the estimate walked made them
    double shareTyped(CheckEdgeTypeConstraint check,
                      llvm::function_ref<Value(Value)> nodeOf,
                      const WalkKnowledge& knowledge) const {
        Operation* const hop = findTypedHop(check.getEdgeTypeIds(), nodeOf);
        const auto walkedEdges = hop ? knowledge._hops.find(hop) : knowledge._hops.end();

        HopEdges edges = walkedEdges != knowledge._hops.end() ? walkedEdges->second : HopEdges {};
        const double walked = countHopEdges(edges);
        if (walked == 0) {
            return 0;
        }

        narrowTypes(check.getEdgeTypes(), edges);

        return countHopEdges(edges) / walked;
    }

    double keepTyped(CheckEdgeTypeConstraint check,
                     llvm::function_ref<Value(Value)> nodeOf,
                     WalkKnowledge& knowledge) const {
        const double kept = shareTyped(check, nodeOf, knowledge);

        Operation* const hop = findTypedHop(check.getEdgeTypeIds(), nodeOf);
        const auto walkedEdges = hop ? knowledge._hops.find(hop) : knowledge._hops.end();
        if (walkedEdges != knowledge._hops.end()) {
            narrowTypes(check.getEdgeTypes(), walkedEdges->second);
        }

        return kept;
    }

    // With nothing to count the values by, one in DEFAULT_DISTINCT_VALUES: PostgreSQL's
    // DEFAULT_EQ_SEL
    static double getEqualityShare(double distinctValues) {
        return distinctValues >= 1 ? 1 / distinctValues : 1 / DEFAULT_DISTINCT_VALUES;
    }

    static double getPropertyDistinctValues(Type element, double nodes) {
        const bool isBoolean = element && (isa<storage::BoolType>(element) || element.isInteger(1));

        return isBoolean ? BOOLEAN_DISTINCT_VALUES : std::min(DEFAULT_DISTINCT_VALUES, nodes);
    }

    // The distinct values a side of an equality holds: one per node in a node column, and
    // none in a value read off no node
    double getDistinctValues(Value side, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        const Value entityColumn = comparedEntityColumn(side);
        GetNodeProperties propertyRead = nodeOf(side).getDefiningOp<GetNodeProperties>();

        if (entityColumn && isNodeColumn(entityColumn)) {
            return getLabelledNodes(knowledge._labels.lookup(nodeOf(entityColumn)));
        } else if (propertyRead) {
            const Value propertyColumn = propertyRead.getResult();
            const double nodes = getLabelledNodes(knowledge._labels.lookup(nodeOf(propertyRead.getInputNodes())));

            return getPropertyDistinctValues(::db::declaredColumnElement(propertyColumn), nodes);
        }

        return 0;
    }

    // An equality keeps one row in the distinct values of the side holding more, as
    // PostgreSQL estimates one over columns it holds no statistics of
    double getEqualitySelectivity(Value lhs, Value rhs, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        const double lhsValues = getDistinctValues(lhs, nodeOf, knowledge);
        const double rhsValues = getDistinctValues(rhs, nodeOf, knowledge);

        return getEqualityShare(std::max(lhsValues, rhsValues));
    }

    double getMembershipSelectivity(InOp membership, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        ConstantOp constant = membership.getRhs().getDefiningOp<ConstantOp>();
        const ArrayAttr elements = constant ? dyn_cast<ArrayAttr>(constant.getValue()) : ArrayAttr();
        if (!elements) {
            return MEMBERSHIP_SELECTIVITY_CEILING;
        }

        const double elementShare = getEqualityShare(getDistinctValues(membership.getLhs(), nodeOf, knowledge));

        return std::min(MEMBERSHIP_SELECTIVITY_CEILING, elementShare * static_cast<double>(elements.size()));
    }

    double getLeafSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
        Operation* const def = predicate.getDefiningOp();

        if (EqOp equality = dyn_cast_or_null<EqOp>(def)) {
            return getEqualitySelectivity(equality.getLhs(), equality.getRhs(), nodeOf, knowledge);
        } else if (NeqOp inequality = dyn_cast_or_null<NeqOp>(def)) {
            return 1 - getEqualitySelectivity(inequality.getLhs(), inequality.getRhs(), nodeOf, knowledge);
        } else if (isa_and_nonnull<GtOp, LtOp, GteOp, LteOp>(def)) {
            return RANGE_SELECTIVITY;
        } else if (InOp membership = dyn_cast_or_null<InOp>(def)) {
            return getMembershipSelectivity(membership, nodeOf, knowledge);
        } else if (CheckEdgeTypeConstraint typeCheck = dyn_cast_or_null<CheckEdgeTypeConstraint>(def)) {
            return shareTyped(typeCheck, nodeOf, knowledge);
        } else if (const LabelCheck check = readLabelCheck(predicate); check._alternatives) {
            return shareLabelled(check._alternatives, nodeOf(check._nodes), knowledge);
        }

        return 1;
    }
};

// The rows an op seeds a factor with, and nothing at all when it seeds none - a fetch, a
// filter or a hop reads a column rather than making one. A listed set of IDs is its own
// count, a literal list one row per element, a by-label scan what the graph holds under
// those labels, and a property scan the share of those an equality keeps.
std::optional<double> estimateSourceRows(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge);

// The ratio an op multiplies the rows it reads by, and nothing when it hands them on as
// they come. A hop makes a row per edge it walks of the nodes it reads, of the types and to
// the labels it keeps, and an undirected one walks both directions. It is a ratio rather
// than a count because a graph with fewer edges than nodes has a hop shrink its input
// rather than grow it. An unwind makes a row per element of each row's list, whose length
// is a property of the rows themselves: no statistic of the graph measures it, so it reads
// at the figure an uncountable source does - where a literal list or a range between
// literal bounds, whose elements are in the IR, is counted exactly instead.
std::optional<double> estimateRowMultiplier(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge);

// The rows a factor makes: the product of what its sources seed it with, a factor crossing
// two of them holding a row per pair, grown or shrunk by every hop and unwind reading from
// one and cut by every filter. A join makes as many as its smaller side, each key taken as
// distinct on its side: the db level counts no distinct values. An aggregate with no
// grouping key makes one row, whatever it reads. A factor seeded from somewhere the db
// level cannot count - a procedure, a load - counts as an uncountable source's rows.
double estimateFactorRows(Region& factor, const RowEstimator& estimator);

// The work a walk does: the rows each op it emits builds, the scan of its first node, then
// each filter and each hop, and the edges each hop reads. A property equality in the first
// filter makes that scan and filter one scan by property value, which emits only the rows
// the filter keeps, and a hop emits only the edges of the types its own filter keeps, as
// the by-type hop it becomes, though it reads every edge of the nodes it leaves. Crossing a
// junction's other side in makes a row per pair, unless it is the hash join fuse_hash_join
// makes of the product and the equality right after it, which reads each side once. An
// equality of a key of each side keeps one pair in as many as the distinct keys of the side
// holding more, as PostgreSQL estimates a join of columns it holds no statistics of.
double estimateWalkCost(const SeedPattern& pattern,
                        Value start,
                        ArrayAttr startLabels,
                        llvm::ArrayRef<WalkEvent> events,
                        const JunctionJoins* joins,
                        const RowEstimator& estimator);

}
