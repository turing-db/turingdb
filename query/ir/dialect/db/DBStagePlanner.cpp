#include "DBStagePlanner.h"

#include <algorithm>
#include <limits>

#include "IRConstantColumn.h"

using namespace mlir;
using namespace mlir::db;

namespace {

// A filter on RowCostOpInterface's scale. It reads its mask on every row it tests and copies each
// column it carries on every row it keeps. Its fixed cost per chunk, spread over the rows of a
// full one, keeps a stage from paying for itself on rows the stages before it all but emptied.
constexpr double FILTER_ROW_COST = 1.0;
constexpr double FILTER_COPY_COST = 1.0;
constexpr double FILTER_STAGE_COST = 0.05;

// A constant is computed once, not per row
double rowCost(Operation* op) {
    if (op->hasTrait<OpTrait::ConstantLike>()) {
        return 0;
    } else if (RowCostOpInterface costed = dyn_cast<RowCostOpInterface>(op)) {
        return costed.getRowCost();
    }

    return 1;
}

}

DBStagePlanner::DBStagePlanner(llvm::function_ref<double(Value)> selectivityOf)
    : _selectivityOf(selectivityOf)
{
}

DBStagePlanner::~DBStagePlanner() {
}

bool DBStagePlanner::plan(FilterOp filter, FilterStages& stages) {
    const Value mask = filter.getMask();
    if (!mask.hasOneUse() || !collectConjunction(mask) || _conjuncts.size() < 2) {
        return false;
    }

    rankConjuncts(filter->getBlock());
    placeConeOps();
    collectCrossings(filter);
    chooseStageStarts(filter.getColumnsToFilter().size());
    if (_stageStarts.size() < 2) {
        return false;
    }

    fillStages(filter, stages);
    return true;
}

// The conjuncts of @param mask, left to right, and the `and`s joining them. False where one of
// the `and`s has a reader besides the filter: its conjuncts are then computed on every row
// whatever the filter keeps.
bool DBStagePlanner::collectConjunction(Value mask) {
    _conjunction.clear();
    _conjuncts.clear();

    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();
        AndOp conjunction = conjunct.getDefiningOp<AndOp>();
        if (!conjunction) {
            _conjuncts.push_back(RankedConjunct {conjunct});
            continue;
        } else if (!conjunct.hasOneUse()) {
            return false;
        }

        _conjunction.insert(conjunction);
        pending.push_back(conjunction.getRhs());
        pending.push_back(conjunction.getLhs());
    }

    return true;
}

// Orders the conjuncts by (selectivity - 1) / cost, which puts the cheap and selective ones first
void DBStagePlanner::rankConjuncts(Block* block) {
    const auto computedInTheBlock = [block](Operation* op) {
        return op->getBlock() == block && computesFromTheRow(op) && !raisesOnSomeRows(op);
    };

    _cones._ops.clear();
    _cones._inputs.clear();
    for (RankedConjunct& conjunct : _conjuncts) {
        _visited.clear();
        conjunct._coneBegin = _cones._ops.size();
        collectConePostOrder(conjunct._value, computedInTheBlock, _visited, _cones);
        conjunct._coneEnd = _cones._ops.size();

        double cost = 0;
        for (Operation* const op : getCone(conjunct)) {
            cost += rowCost(op);
        }

        conjunct._selectivity = _selectivityOf(conjunct._value);
        conjunct._rank = cost == 0 ? -std::numeric_limits<double>::infinity() : (conjunct._selectivity - 1.0) / cost;
    }

    std::stable_sort(_conjuncts.begin(), _conjuncts.end(), [](const RankedConjunct& lhs, const RankedConjunct& rhs) {
        return lhs._rank < rhs._rank;
    });
}

void DBStagePlanner::placeConeOps() {
    _placements.clear();
    for (size_t position = 0; position < _conjuncts.size(); position++) {
        for (Operation* const op : getCone(_conjuncts[position])) {
            _placements.try_emplace(op, ConePlacement {position, false});
        }
    }

    _pending.clear();
    for (size_t position = 0; position < _conjuncts.size(); position++) {
        for (Operation* const op : getCone(_conjuncts[position])) {
            ConePlacement& placement = _placements.find(op)->second;
            if (placement._position != position || placement._computedBefore) {
                continue;
            }

            const bool readByAnotherOp = llvm::any_of(op->getUsers(), [this](Operation* user) {
                return !_placements.contains(user) && !_conjunction.contains(user);
            });

            if (readByAnotherOp) {
                placement._computedBefore = true;
                _pending.push_back(op);
            }
        }
    }

    while (!_pending.empty()) {
        Operation* const op = _pending.pop_back_val();
        for (const Value operand : op->getOperands()) {
            const auto found = _placements.find(operand.getDefiningOp());
            if (found != _placements.end() && !found->second._computedBefore) {
                found->second._computedBefore = true;
                _pending.push_back(found->first);
            }
        }
    }

    for (size_t position = 0; position < _conjuncts.size(); position++) {
        RankedConjunct& conjunct = _conjuncts[position];
        for (Operation* const op : getCone(conjunct)) {
            size_t stage = 0;
            if (findStageOf(op, stage) && stage == position) {
                conjunct._ownCost += rowCost(op);
            }
        }
    }
}

// The values a conjunct reads, through its own value or the ops its stage computes for it, that
// a conjunct before it or the code before the filter computes
void DBStagePlanner::collectCrossings(FilterOp filter) {
    _crossings.clear();
    _crossingIndices.clear();

    Block* const block = filter->getBlock();
    const Operation::operand_range columns = filter.getColumnsToFilter();
    const auto noteRead = [this, block, &columns](Value value, size_t position) {
        if (::db::yieldsConstantColumn(value, _constantColumns) || llvm::is_contained(columns, value)) {
            return;
        }

        size_t stage = 0;
        const size_t crossesFrom = findStageOf(value.getDefiningOp(), stage) ? stage + 1 : 0;
        if (crossesFrom > position) {
            return;
        }

        const auto [found, inserted] = _crossingIndices.try_emplace(value, _crossings.size());
        if (inserted) {
            const bool carriable = isa<ColumnType>(value.getType()) && value.getParentBlock() == block;
            _crossings.push_back(StageCrossing {value, crossesFrom, position, carriable});
        } else {
            StageCrossing& crossing = _crossings[found->second];
            crossing._lastRead = std::max(crossing._lastRead, position);
        }
    };

    for (size_t position = 0; position < _conjuncts.size(); position++) {
        const RankedConjunct& conjunct = _conjuncts[position];
        noteRead(conjunct._value, position);

        for (Operation* const op : getCone(conjunct)) {
            size_t stage = 0;
            if (!findStageOf(op, stage) || stage != position) {
                continue;
            }

            for (const Value operand : op->getOperands()) {
                noteRead(operand, position);
            }
        }
    }
}

// Stages of the ranked conjuncts are segments between boundaries: a stage starting at conjunct i
// computes conjuncts i to j - 1 and runs a filter on the rows reaching i, and its filter copies
// the filter's columns and those a later stage reads on the rows reaching j
void DBStagePlanner::chooseStageStarts(size_t columnCount) {
    const size_t count = _conjuncts.size();

    _crossingChanges.assign(count + 1, CrossingChange {});
    for (const StageCrossing& crossing : _crossings) {
        const size_t first = std::max<size_t>(1, crossing._crossesFrom);
        if (first > crossing._lastRead) {
            continue;
        }

        _crossingChanges[first]._carried++;
        _crossingChanges[crossing._lastRead + 1]._carried--;
        if (!crossing._carriable) {
            _crossingChanges[first]._blocked++;
            _crossingChanges[crossing._lastRead + 1]._blocked--;
        }
    }

    _solver.clear();
    double rows = 1.0;
    double spent = 0.0;
    int64_t carried = 0;
    int64_t blocked = 0;
    for (size_t boundary = 0; boundary <= count; boundary++) {
        if (boundary > 0) {
            const RankedConjunct& conjunct = _conjuncts[boundary - 1];
            const CrossingChange& change = _crossingChanges[boundary];
            rows *= conjunct._selectivity;
            spent += conjunct._ownCost;
            carried += change._carried;
            blocked += change._blocked;
        }

        const bool isLast = boundary == count;
        const double copiedColumns = static_cast<double>(columnCount + static_cast<size_t>(carried));

        ::db::SegmentationSolver::Boundary stageBoundary;
        stageBoundary._position = spent;
        stageBoundary._weight = rows;
        stageBoundary._startCost = rows * FILTER_ROW_COST + FILTER_STAGE_COST;
        stageBoundary._endCost = isLast ? 0.0 : rows * FILTER_COPY_COST * copiedColumns;
        stageBoundary._cuttable = !isLast && blocked == 0;
        _solver.addBoundary(stageBoundary);
    }

    _solver.getSegmentStarts(_stageStarts);
}

void DBStagePlanner::fillStages(FilterOp filter, FilterStages& stages) {
    stages._filter = filter;
    for (const RankedConjunct& conjunct : _conjuncts) {
        stages._conjuncts.push_back(conjunct._value);
    }

    _visited.clear();
    for (size_t index = 0; index < _stageStarts.size(); index++) {
        const size_t begin = _stageStarts[index];
        const size_t end = index + 1 < _stageStarts.size() ? _stageStarts[index + 1] : _conjuncts.size();

        if (begin > 0) {
            const auto computedInTheStage = [this, begin, end](Operation* op) {
                size_t stage = 0;
                return findStageOf(op, stage) && stage >= begin && stage < end;
            };

            for (size_t position = begin; position < end; position++) {
                collectConePostOrder(_conjuncts[position]._value, computedInTheStage, _visited, stages._moved);
            }
        }

        if (end < _conjuncts.size()) {
            for (const StageCrossing& crossing : _crossings) {
                if (crossesBoundary(crossing, end)) {
                    stages._carried.push_back(crossing._value);
                }
            }
        }

        stages._stageEnds.push_back(StageEnd {end, stages._moved._ops.size(), stages._carried.size()});
    }
}

llvm::ArrayRef<Operation*> DBStagePlanner::getCone(const RankedConjunct& conjunct) const {
    return llvm::ArrayRef<Operation*>(_cones._ops).slice(conjunct._coneBegin, conjunct._coneEnd - conjunct._coneBegin);
}

bool DBStagePlanner::findStageOf(Operation* op, size_t& position) const {
    const auto found = _placements.find(op);
    if (found == _placements.end() || found->second._computedBefore) {
        return false;
    }

    position = found->second._position;
    return true;
}

bool DBStagePlanner::crossesBoundary(const StageCrossing& crossing, size_t boundary) {
    return std::max<size_t>(1, crossing._crossesFrom) <= boundary && boundary <= crossing._lastRead;
}
