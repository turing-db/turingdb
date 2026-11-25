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
