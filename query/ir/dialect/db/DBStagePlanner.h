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
#include "SegmentationSolver.h"

namespace mlir::db {

// Splits the conjuncts of a filter into stages, each over the rows the stages before it keep:
// the cheap and selective conjuncts first, in the order of Hellerstein's rank, and a cut where a
// filter between two stages saves more than it costs. One planner plans filter after filter.
class DBStagePlanner {
public:
    // Where a stage ends in each list of its filter's stages
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
        MaskCone _moved;
        llvm::SmallVector<Value, 4> _carried;
        llvm::SmallVector<StageEnd, 2> _stageEnds;
    };

    // @param selectivityOf gives the share of the rows it tests a predicate keeps, and outlives
    // the planner
    explicit DBStagePlanner(llvm::function_ref<double(Value)> selectivityOf);
    ~DBStagePlanner();

    // False where the filter stays one stage
    bool plan(FilterOp filter, FilterStages& stages);

private:
    // A conjunct of the mask: the share of rows it keeps, its rank, and the cost of the ops it
    // is the first conjunct to need. Its cone is a run of `_cones`.
    struct RankedConjunct {
        Value _value;
        double _selectivity {1.0};
        double _rank {0.0};
        double _ownCost {0.0};
        size_t _coneBegin {0};
        size_t _coneEnd {0};
    };

    // Where an op of the cones is computed: in the stage of the first conjunct needing it, or
    // before the filter when an op besides the conjuncts reads it or one of the ops it feeds
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

    // How the count of values crossing a boundary changes from the boundary before
    struct CrossingChange {
        int64_t _carried {0};
        int64_t _blocked {0};
    };

    llvm::function_ref<double(Value)> _selectivityOf;
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

    bool collectConjunction(Value mask);
    void rankConjuncts(Block* block);
    void placeConeOps();
    void collectCrossings(FilterOp filter);
    void chooseStageStarts(size_t columnCount);
    void fillStages(FilterOp filter, FilterStages& stages);

    llvm::ArrayRef<Operation*> getCone(const RankedConjunct& conjunct) const;

    // The position of the conjunct whose stage computes @param op, false where none does
    bool findStageOf(Operation* op, size_t& position) const;

    static bool crossesBoundary(const StageCrossing& crossing, size_t boundary);
};

}
