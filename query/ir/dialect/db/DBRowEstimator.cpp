#include "DBRowEstimator.h"

#include <math.h>

#include "mlir/IR/ValueRange.h"

#include "IRConstantColumn.h"
#include "DBPatternWalk.h"
#include "DBRowCone.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// What the db level puts on rows it cannot count - the figure the v2 planner reads a
// yielded item at.
constexpr size_t uncountableRows = 10;

std::optional<int64_t> literalInteger(Value value) {
    ConstantOp constant = value.getDefiningOp<ConstantOp>();
    const IntegerAttr integer = constant ? dyn_cast<IntegerAttr>(constant.getValue()) : IntegerAttr();
    if (!integer) {
        return std::nullopt;
    }

    return integer.getInt();
}

// A list bound when the query runs has a length no statistic of the graph measures, so it
// reads at the figure an uncountable source does
double estimateListedEntities(size_t literalCount, mlir::OperandRange boundIDs) {
    double listed = static_cast<double>(literalCount);
    for (const Value bound : boundIDs) {
        const bool listsIDs = isa<storage::ListType>(::db::declaredColumnElement(bound));
        listed += listsIDs ? static_cast<double>(uncountableRows) : 1.0;
    }

    return listed;
}

struct RegionRows {
    double _rows {1};
    bool _countedASource {false};
};

double estimateRegionRows(const RegionRows& region) {
    return region._countedASource ? region._rows : static_cast<double>(uncountableRows);
}

bool fusesIntoScanByPropertyValue(FilterOp filter, Value start, const SeedPattern& pattern) {
    const auto readsTheStart = [&pattern, start](Value column) {
        return patternOrigin(column, pattern) == start;
    };

    const MaskCone cone = collectMaskCone(filter.getMask());
    const bool readsTheStartAlone = llvm::all_of(cone._inputs, [&readsTheStart](Value input) {
        return readsTheStart(input) || ::db::isRunInvariant(input);
    });

    if (!readsTheStartAlone) {
        return false;
    }

    llvm::SmallVector<Value> conjuncts;
    collectConjuncts(filter.getMask(), conjuncts);

    return llvm::any_of(conjuncts, [&readsTheStart](Value conjunct) {
        EqOp equality = conjunct.getDefiningOp<EqOp>();
        PropertyEquality property;
        return equality && matchPropertyEquality(equality, property) && readsTheStart(property._read.getInputNodes());
    });
}

// The types each hop of the pattern keeps by a filter of its own type column, which a later
// pass fuses into the hop
void collectHopTypeChecks(const SeedPattern& pattern, llvm::DenseMap<Operation*, ArrayAttr>& hopTypes) {
    for (FilterOp filter : pattern._filters) {
        CheckEdgeTypeConstraint check = filter.getMask().getDefiningOp<CheckEdgeTypeConstraint>();
        const OpResult edgeTypes = check ? dyn_cast<OpResult>(check.getEdgeTypeIds()) : OpResult();
        if (!edgeTypes) {
            continue;
        }

        Operation* const hop = edgeTypes.getOwner();
        if (isEdgeHop(hop) && edgeTypes.getResultNumber() == EDGE_TYPES_RESULT) {
            hopTypes[hop] = check.getEdgeTypes();
        }
    }
}

}

bool mlir::db::collectScanLabels(ArrayAttr labelNames, const ::db::GraphMetadata& metadata, ::db::LabelSet& labels) {
    const ::db::LabelMap& labelMap = metadata.labels();
    bool resolvesEveryLabel = true;
    for (const Attribute labelAttr : labelNames) {
        const StringAttr nameAttr = dyn_cast<StringAttr>(labelAttr);
        if (!nameAttr) {
            continue;
        }

        const llvm::StringRef name = nameAttr.getValue();
        const std::optional<::db::LabelID> label = labelMap.get(std::string_view(name.data(), name.size()));

        if (label) {
            labels.set(*label);
        } else {
            resolvesEveryLabel = false;
        }
    }

    return resolvesEveryLabel;
}

size_t mlir::db::estimateIndexedEdges(ArrayAttr labelNames,
                                      bool indexedByTarget,
                                      const ::db::GraphMetadata& metadata,
                                      const ::db::CardinalityEstimation& estimation) {
    ::db::LabelSet labels;
    if (!collectScanLabels(labelNames, metadata, labels)) {
        return 0;
    } else if (indexedByTarget) {
        return estimation.estimateInEdgeCount(labels);
    }

    return estimation.estimateOutEdgeCount(labels);
}

double mlir::db::estimateUnwoundRows(Unwind unwind) {
    Range range = unwind.getSource().getDefiningOp<Range>();
    if (!range) {
        return static_cast<double>(uncountableRows);
    }

    const Value stepColumn = range.getStep();
    const std::optional<int64_t> start = literalInteger(range.getStart());
    const std::optional<int64_t> end = literalInteger(range.getEnd());
    const std::optional<int64_t> step = stepColumn ? literalInteger(stepColumn) : std::optional<int64_t>(1);

    const bool countsLiterals = start && end && step && *step != 0;
    if (!countsLiterals) {
        return static_cast<double>(uncountableRows);
    }

    const double steps = (static_cast<double>(*end) - static_cast<double>(*start)) / static_cast<double>(*step);

    return steps < 0 ? 0 : floor(steps) + 1;
}

void mlir::db::collectEdgeTypes(ArrayAttr names, const ::db::GraphMetadata& metadata, llvm::SmallVectorImpl<::db::EdgeTypeID>& edgeTypes) {
    const ::db::EdgeTypeMap& edgeTypeMap = metadata.edgeTypes();
    for (const Attribute nameAttr : names) {
        const StringAttr name = dyn_cast<StringAttr>(nameAttr);
        if (!name) {
            continue;
        }

        const llvm::StringRef nameRef = name.getValue();
        const std::optional<::db::EdgeTypeID> edgeType = edgeTypeMap.get(std::string_view(nameRef.data(), nameRef.size()));
        if (edgeType && !llvm::is_contained(edgeTypes, *edgeType)) {
            edgeTypes.push_back(*edgeType);
        }
    }
}

Operation* mlir::db::findTypedHop(Value edgeTypes, llvm::function_ref<Value(Value)> nodeOf) {
    Value column = nodeOf(edgeTypes);
    size_t resultIndex = EDGE_TYPES_RESULT;
    if (GetEdgeTypes read = column.getDefiningOp<GetEdgeTypes>()) {
        column = nodeOf(read.getInputEdges());
        resultIndex = EDGE_IDS_RESULT;
    }

    const OpResult result = dyn_cast<OpResult>(column);
    if (!result || result.getResultNumber() != resultIndex) {
        return nullptr;
    }

    Operation* const owner = result.getOwner();
    if (!isEdgeHop(owner) && !isEdgeScan(owner)) {
        return nullptr;
    }

    return owner;
}

void WalkKnowledge::handOn(Operation* op) {
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

void WalkKnowledge::handOn(Value rowSource, Value column) {
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

RowEstimator::RowEstimator(const ::db::GraphView& view)
    : _metadata(view.metadata()),
    _estimation(view),
    _nodeCount(static_cast<double>(_estimation.estimateNodeCount(::db::LabelSet {}))),
    _edgeCount(static_cast<double>(_estimation.estimateEdgeCount()))
{
}

double RowEstimator::getLabelledNodes(const ::db::LabelSet& labels) const {
    if (labels.empty()) {
        return _nodeCount;
    }

    return static_cast<double>(_estimation.estimateNodeCount(labels));
}

double RowEstimator::scanNodes(Value nodes, ArrayAttr labelNames, WalkKnowledge& knowledge) const {
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

double RowEstimator::scanNodesByPropertyValue(ScanNodesByPropertyValue scan, WalkKnowledge& knowledge) const {
    const std::optional<ArrayAttr> labels = scan.getLabels();
    const double scanned = scanNodes(scan.getResult(), labels ? *labels : ArrayAttr(), knowledge);

    const TypedAttr value = scan.getValueAttr();
    const Type element = value ? value.getType() : ::db::declaredColumnElement(scan.getBoundValue());

    return scanned * getEqualityShare(getPropertyDistinctValues(element, scanned));
}

double RowEstimator::scanEdges(Operation* scan,
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

HopEstimate RowEstimator::walkHop(Operation* hop,
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

double RowEstimator::keepLabelled(ArrayAttr alternatives, Value node, WalkKnowledge& knowledge) const {
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

double RowEstimator::getSelectivity(Value mask, llvm::function_ref<Value(Value)> nodeOf, WalkKnowledge& knowledge) const {
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

double RowEstimator::getPredicateSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
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

RowEstimator::LabelCheck RowEstimator::readLabelCheck(Value predicate) {
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

void RowEstimator::appendLabels(const ::db::LabelSet& labels, StatisticKey& key) {
    for (const uint64_t word : labels.integers()) {
        key.push_back(word);
    }
}

void RowEstimator::appendDegree(const ::db::DegreeSelection& degree, StatisticKey& key) {
    key.push_back(degree._isOut ? 1 : 0);
    key.push_back(degree._isEveryType ? 1 : 0);
    key.push_back(degree._edgeTypes.size());
    for (const ::db::EdgeTypeID edgeType : degree._edgeTypes) {
        key.push_back(edgeType.getValue());
    }
}

double RowEstimator::countEdges(const ::db::LabelSet& source, const HopEdges& edges, const ::db::LabelSet& target) const {
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

double RowEstimator::sumDegreeProducts(const ::db::LabelSet& labels,
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

double RowEstimator::countHopEdges(const HopEdges& edges) const {
    const ::db::LabelSet& source = edges._leavesSource ? edges._near : edges._far;
    const ::db::LabelSet& target = edges._leavesSource ? edges._far : edges._near;

    double count = countEdges(source, edges, target);
    if (edges._walksBoth) {
        count += countEdges(target, edges, source);
    }

    return count;
}

::db::DegreeSelection RowEstimator::selectionOf(const HopEdges& edges, bool isOut) {
    return ::db::DegreeSelection {._edgeTypes = edges._types, ._isEveryType = !edges._isTyped, ._isOut = isOut};
}

double RowEstimator::countSelectedEdges(const ::db::LabelSet& labels, const ::db::DegreeSelection& degree) const {
    HopEdges edges;
    edges._near = labels;
    edges._types.assign(degree._edgeTypes.begin(), degree._edgeTypes.end());
    edges._isTyped = !degree._isEveryType;
    edges._leavesSource = degree._isOut;

    return countHopEdges(edges);
}

double RowEstimator::getDegreeLift(const ::db::LabelSet& labels,
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

void RowEstimator::narrowTypes(ArrayAttr typeNames, HopEdges& edges) const {
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

void RowEstimator::addSharedLabels(ArrayAttr alternatives, ::db::LabelSet& labels) const {
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

double RowEstimator::shareLabelled(ArrayAttr alternatives, const ::db::LabelSet& known) const {
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

double RowEstimator::shareLabelled(ArrayAttr alternatives, Value node, const WalkKnowledge& knowledge) const {
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

double RowEstimator::shareTyped(CheckEdgeTypeConstraint check,
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

double RowEstimator::keepTyped(CheckEdgeTypeConstraint check,
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

double RowEstimator::getEqualityShare(double distinctValues) {
    return distinctValues >= 1 ? 1 / distinctValues : 1 / DEFAULT_DISTINCT_VALUES;
}

double RowEstimator::getPropertyDistinctValues(Type element, double nodes) {
    const bool isBoolean = element && (isa<storage::BoolType>(element) || element.isInteger(1));

    return isBoolean ? BOOLEAN_DISTINCT_VALUES : std::min(DEFAULT_DISTINCT_VALUES, nodes);
}

double RowEstimator::getDistinctValues(Value side, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
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

double RowEstimator::getEqualitySelectivity(Value lhs, Value rhs, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
    const double lhsValues = getDistinctValues(lhs, nodeOf, knowledge);
    const double rhsValues = getDistinctValues(rhs, nodeOf, knowledge);

    return getEqualityShare(std::max(lhsValues, rhsValues));
}

double RowEstimator::getMembershipSelectivity(InOp membership, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
    ConstantOp constant = membership.getRhs().getDefiningOp<ConstantOp>();
    const ArrayAttr elements = constant ? dyn_cast<ArrayAttr>(constant.getValue()) : ArrayAttr();
    if (!elements) {
        return MEMBERSHIP_SELECTIVITY_CEILING;
    }

    const double elementShare = getEqualityShare(getDistinctValues(membership.getLhs(), nodeOf, knowledge));

    return std::min(MEMBERSHIP_SELECTIVITY_CEILING, elementShare * static_cast<double>(elements.size()));
}

double RowEstimator::getLeafSelectivity(Value predicate, llvm::function_ref<Value(Value)> nodeOf, const WalkKnowledge& knowledge) const {
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

std::optional<double> mlir::db::estimateSourceRows(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge) {
    if (ConstScanNodes constScan = dyn_cast<ConstScanNodes>(op)) {
        return estimateListedEntities(constScan.getNodeIDs().size(), constScan.getBoundIDs());
    } else if (ConstScanEdges constScan = dyn_cast<ConstScanEdges>(op)) {
        const double listed = estimateListedEntities(constScan.getEdgeIDs().size(), constScan.getBoundIDs());
        return constScan.getUndirected() ? 2 * listed : listed;
    } else if (UnwindConst unwind = dyn_cast<UnwindConst>(op)) {
        return static_cast<double>(unwind.getElements().size());
    } else if (ScanNodesByLabel byLabel = dyn_cast<ScanNodesByLabel>(op)) {
        return estimator.scanNodes(byLabel.getResult(), byLabel.getLabels(), knowledge);
    } else if (ScanNodes scan = dyn_cast<ScanNodes>(op)) {
        return estimator.scanNodes(scan.getResult(), ArrayAttr(), knowledge);
    } else if (ScanNodesByPropertyValue byValue = dyn_cast<ScanNodesByPropertyValue>(op)) {
        return estimator.scanNodesByPropertyValue(byValue, knowledge);
    } else if (EdgeScanLabels scanLabels; readEdgeScanLabels(op, scanLabels)) {
        return estimator.scanEdges(op, scanLabels._source, scanLabels._target, ArrayAttr(), knowledge);
    } else if (ScanEdgesByType byType = dyn_cast<ScanEdgesByType>(op)) {
        return estimator.scanEdges(op, ArrayAttr(), ArrayAttr(), byType.getEdgeTypes(), knowledge);
    }

    return std::nullopt;
}

std::optional<double> mlir::db::estimateRowMultiplier(Operation* op, const RowEstimator& estimator, WalkKnowledge& knowledge) {
    if (Unwind unwind = dyn_cast<Unwind>(op)) {
        return estimateUnwoundRows(unwind);
    } else if (!isEdgeHop(op)) {
        return std::nullopt;
    }

    return estimator.walkHop(op, false, op->getOperand(0), op->getResult(hopReachedResult(op)), ArrayAttr(), knowledge)._rows;
}

double mlir::db::estimateFactorRows(Region& factor, const RowEstimator& estimator) {
    llvm::DenseMap<Region*, RegionRows> regionRows;
    WalkKnowledge knowledge;

    const auto columnItself = [](Value column) {
        return column;
    };

    const auto countRows = [&](Operation* op) {
        RegionRows nested;
        if (HashJoin join = dyn_cast<HashJoin>(op)) {
            const double leftRows = estimateRegionRows(regionRows.lookup(&join.getLeftFactor()));
            const double rightRows = estimateRegionRows(regionRows.lookup(&join.getRightFactor()));
            nested = RegionRows {std::min(leftRows, rightRows), true};
        } else {
            for (Region& region : op->getRegions()) {
                const RegionRows counted = regionRows.lookup(&region);
                nested._rows *= counted._rows;
                nested._countedASource = nested._countedASource || counted._countedASource;
            }
        }

        RegionRows& rows = regionRows[op->getParentRegion()];
        rows._rows *= nested._rows;
        rows._countedASource = rows._countedASource || nested._countedASource;

        if (FilterOp filter = dyn_cast<FilterOp>(op)) {
            rows._rows *= estimator.getSelectivity(filter.getMask(), columnItself, knowledge);
            return;
        }

        AggregateOpInterface aggregate = dyn_cast<AggregateOpInterface>(op);
        if (aggregate && aggregate.getGroupingKeys().empty()) {
            rows = RegionRows {1, true};
            return;
        }

        const std::optional<double> seeded = estimateSourceRows(op, estimator, knowledge);
        if (seeded) {
            rows._countedASource = true;
            rows._rows *= *seeded;
            return;
        }

        const std::optional<double> multiplier = estimateRowMultiplier(op, estimator, knowledge);
        if (multiplier) {
            rows._rows *= *multiplier;
        }
    };

    factor.walk([&](Operation* op) {
        countRows(op);
        knowledge.handOn(op);
    });

    return estimateRegionRows(regionRows.lookup(&factor));
}

double mlir::db::estimateWalkCost(const SeedPattern& pattern,
                                  Value start,
                                  ArrayAttr startLabels,
                                  llvm::ArrayRef<WalkEvent> events,
                                  const JunctionJoins* joins,
                                  const RowEstimator& estimator) {
    const auto nodeOf = [&pattern, joins](Value column) {
        const Value patternColumn = joins ? junctionSource(joins->_junction, climbFilters(column)) : Value();
        return patternOrigin(patternColumn ? patternColumn : column, pattern);
    };

    llvm::DenseMap<Operation*, ArrayAttr> hopTypes;
    collectHopTypeChecks(pattern, hopTypes);

    WalkKnowledge knowledge;
    double rows = estimator.getNodeCount();
    if (startLabels) {
        rows *= estimator.keepLabelled(ArrayAttr::get(startLabels.getContext(), {startLabels}), start, knowledge);
    }

    const bool startsWithFilter = !events.empty() && events.front()._kind == WalkEvent::Kind::Filter;
    const bool scansByPropertyValue = startsWithFilter && fusesIntoScanByPropertyValue(events.front()._filter, start, pattern);

    double cost = scansByPropertyValue ? 0 : rows;
    double crossedPatternRows = 0;
    for (size_t index = 0; index < events.size(); index++) {
        const WalkEvent& event = events[index];
        bool buildsRows = true;

        switch (event._kind) {
            case WalkEvent::Kind::RootLabels: {
                const ArrayAttr rootLabels = cast<ScanNodesByLabel>(pattern._root).getLabels();
                rows *= estimator.keepLabelled(ArrayAttr::get(rootLabels.getContext(), {rootLabels}), pattern._root->getResult(0), knowledge);
            }
            break;
            case WalkEvent::Kind::Filter: {
                FilterOp filter = event._filter;
                rows *= estimator.getSelectivity(filter.getMask(), nodeOf, knowledge);
            }
            break;
            case WalkEvent::Kind::Forward: {
                const PatternStep& step = *event._step;
                const HopEstimate hop = estimator.walkHop(step._op, false, step._from, step._to, hopTypes.lookup(step._op), knowledge);
                cost += rows * hop._edgesRead;
                rows *= hop._rows;
            }
            break;
            case WalkEvent::Kind::Backward: {
                const PatternStep& step = *event._step;
                const HopEstimate hop = estimator.walkHop(step._op, true, step._to, step._from, hopTypes.lookup(step._op), knowledge);
                cost += rows * hop._edgesRead;
                rows *= hop._rows;
            }
            break;
            case WalkEvent::Kind::Cross: {
                const bool joinsNext = index + 1 < events.size() && events[index + 1]._kind == WalkEvent::Kind::Join;
                const bool hashes = joinsNext && events[index + 1]._join->_hashes;
                if (hashes) {
                    cost += rows + joins->_crossedRows;
                    buildsRows = false;
                }

                crossedPatternRows = rows;
                rows *= joins->_crossedRows;
            }
            break;
            case WalkEvent::Kind::Join: {
                const JoinConjunct& join = *event._join;
                if (join._equatesKeys) {
                    const double patternKeys = std::min(DEFAULT_DISTINCT_VALUES, crossedPatternRows);
                    const double otherKeys = std::min(DEFAULT_DISTINCT_VALUES, joins->_crossedRows);
                    rows /= std::max({patternKeys, otherKeys, 1.0});
                } else {
                    rows *= estimator.getSelectivity(join._conjunct, nodeOf, knowledge);
                }
            }
            break;
        }

        if (buildsRows) {
            cost += rows;
        }
    }

    return cost;
}
