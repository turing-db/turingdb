#pragma once

#include <stddef.h>
#include <stdint.h>

#include <vector>

#include "mlir/IR/Value.h"
#include "llvm/ADT/ArrayRef.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLFunctionalExtras.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBRowCone.h"
#include "DBWrittenOrder.h"
#include "SegmentationSolver.h"

namespace mlir::db {

// Splits the conjuncts of a filter into stages, each over the rows the stages before it keep:
// the cheap and selective conjuncts first, in the order of Hellerstein's rank, and a cut where a
// filter between two stages saves more than it costs. One planner plans filter after filter.
class DBStagePlanner {
public:
    struct StageEnd {
        size_t _conjuncts {0};
        size_t _ops {0};
        size_t _carried {0};
    };

    // The stages of a filter: its conjuncts, the ops a later stage computes after the filter
    // before it, and the columns each stage's filter carries beside the filter's own
    struct FilterStages {
        FilterOp _filter;
        llvm::SmallVector<Value, 4> _conjuncts;
        llvm::SmallVector<Operation*> _moved;
        llvm::SmallVector<Value, 4> _carried;
        llvm::SmallVector<StageEnd, 2> _stageEnds;
    };

    explicit DBStagePlanner(const DBWrittenOrder* writtenOrder);
    ~DBStagePlanner();

    // @param selectivityOf gives the share of the rows it tests a predicate keeps
    bool plan(FilterOp filter, llvm::function_ref<double(Value)> selectivityOf, FilterStages& stages);

private:
    struct RankedConjunct {
        Value _value;
        double _selectivity {1.0};
        double _rank {0.0};
        double _ownCost {0.0};
        size_t _coneBegin {0};
        size_t _coneEnd {0};
    };

    // Where an op of the cones is computed: in the stage of the first conjunct needing it, or
    // before the filter when an op besides the conjuncts reads it or one of the ops it feeds, or
    // when a graph write lies between it and the filter
    struct ConePlacement {
        size_t _position {0};
        bool _computedBefore {false};
    };

    // A value read at a later conjunct than the one it is computed at: the stage boundaries
    // from `_crossesFrom` to `_lastRead` have their filter carry it, 0 standing for a value
    // computed before the filter
    struct StageCrossing {
        Value _value;
        size_t _crossesFrom {0};
        size_t _lastRead {0};
        bool _carriable {false};
    };

    struct CrossingChange {
        int64_t _carried {0};
        int64_t _blocked {0};
    };

    const DBWrittenOrder* _writtenOrder {nullptr};
    ::db::SegmentationSolver _solver;
    llvm::DenseMap<Value, bool> _constantColumns;
    llvm::SmallPtrSet<Operation*, 8> _conjunction;
    llvm::SmallVector<RankedConjunct, 4> _conjuncts;
    MaskCone _cones;
    llvm::SmallPtrSet<Operation*, 8> _visited;
    llvm::DenseMap<Operation*, ConePlacement> _placements;
    llvm::SmallVector<Operation*> _pending;
    llvm::SmallVector<StageCrossing> _crossings;
    llvm::DenseMap<Value, size_t> _crossingIndices;
    llvm::SmallVector<CrossingChange> _crossingChanges;
    std::vector<size_t> _stageStarts;

    void collectConjunction(Value mask);
    void collectCones(Block* block);
    void markComputedBefore(Operation* filter);
    void rankConjuncts(llvm::function_ref<double(Value)> selectivityOf);
    void placeConeOps();
    void collectCrossings(Block* block, Operation::operand_range columns);
    void chooseStageStarts(size_t columnCount);
    void fillStages(FilterOp filter, FilterStages& stages);
    double rowCostOf(Operation* op);

    llvm::ArrayRef<Operation*> getCone(const RankedConjunct& conjunct) const;

    bool findStageOf(Operation* op, size_t& position) const;
    bool isComputedAt(Operation* op, size_t position) const;

    static size_t getFirstCrossedBoundary(const StageCrossing& crossing);
    static bool crossesBoundary(const StageCrossing& crossing, size_t boundary);
};

}
