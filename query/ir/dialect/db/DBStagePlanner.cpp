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

}

DBStagePlanner::DBStagePlanner(const DBWrittenOrder* writtenOrder)
    : _writtenOrder(writtenOrder)
{
}

DBStagePlanner::~DBStagePlanner() {
}

bool DBStagePlanner::plan(FilterOp filter, llvm::function_ref<double(Value)> selectivityOf, FilterStages& stages) {
    collectConjunction(filter.getMask());
    if (_conjuncts.size() < 2) {
        return false;
    }

    Operation* const filterOp = filter.getOperation();
    Block* const block = filterOp->getBlock();
    const Operation::operand_range columns = filter.getColumnsToFilter();

    collectCones(block);
    markComputedBefore(filterOp);
    rankConjuncts(selectivityOf);
    placeConeOps();
    collectCrossings(block, columns);
    chooseStageStarts(columns.size());
    if (_stageStarts.size() < 2) {
        return false;
    }

    fillStages(filter, stages);
    return true;
}

// The conjuncts of @param mask, left to right, and the `and`s joining them. An `and` with a reader
// besides the filter is computed on every row whatever the filter keeps, so it is one conjunct.
void DBStagePlanner::collectConjunction(Value mask) {
    _conjunction.clear();
    _conjuncts.clear();

    llvm::SmallVector<Value> pending {mask};
    while (!pending.empty()) {
        const Value conjunct = pending.pop_back_val();
        AndOp conjunction = conjunct.getDefiningOp<AndOp>();
        if (!conjunction || !conjunct.hasOneUse()) {
            _conjuncts.push_back(RankedConjunct {conjunct});
            continue;
        }

        _conjunction.insert(conjunction);
        pending.push_back(conjunction.getRhs());
        pending.push_back(conjunction.getLhs());
    }
}

void DBStagePlanner::collectCones(Block* block) {
    _cones._ops.clear();
    _cones._inputs.clear();
    for (RankedConjunct& conjunct : _conjuncts) {
        conjunct._coneBegin = _cones._ops.size();
        collectRowCone({conjunct._value}, block, _cones);
        conjunct._coneEnd = _cones._ops.size();
    }
}

// Orders the conjuncts by (selectivity - 1) / cost, which puts the cheap and selective ones first
void DBStagePlanner::rankConjuncts(llvm::function_ref<double(Value)> selectivityOf) {
    for (RankedConjunct& conjunct : _conjuncts) {
        double cost = 0;
        for (Operation* const op : getCone(conjunct)) {
            if (!_placements.find(op)->second._computedBefore) {
                cost += rowCostOf(op);
            }
        }

        conjunct._selectivity = selectivityOf(conjunct._value);
        conjunct._rank = cost == 0 ? -std::numeric_limits<double>::infinity() : (conjunct._selectivity - 1.0) / cost;
    }

    std::stable_sort(_conjuncts.begin(), _conjuncts.end(), [](const RankedConjunct& lhs, const RankedConjunct& rhs) {
        return lhs._rank < rhs._rank;
    });
}

void DBStagePlanner::markComputedBefore(Operation* filter) {
    _placements.clear();
    for (Operation* const op : _cones._ops) {
        _placements.try_emplace(op, ConePlacement {});
    }

    _visited.clear();
    _pending.clear();
    for (Operation* const op : _cones._ops) {
        if (!_visited.insert(op).second) {
            continue;
        }

        const bool readByAnotherOp = llvm::any_of(op->getUsers(), [this](Operation* user) {
            return !_placements.contains(user) && !_conjunction.contains(user);
        });

        const bool computedBeforeAWrite = _writtenOrder->writesBetween(op, filter);
        if (readByAnotherOp || computedBeforeAWrite) {
            _placements.find(op)->second._computedBefore = true;
            _pending.push_back(op);
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
}

void DBStagePlanner::placeConeOps() {
    const size_t count = _conjuncts.size();
    for (size_t index = 0; index < count; index++) {
        const size_t position = count - 1 - index;
        for (Operation* const op : getCone(_conjuncts[position])) {
            _placements.find(op)->second._position = position;
        }
    }

    for (size_t position = 0; position < count; position++) {
        RankedConjunct& conjunct = _conjuncts[position];
        for (Operation* const op : getCone(conjunct)) {
            if (isComputedAt(op, position)) {
                conjunct._ownCost += rowCostOf(op);
            }
        }
    }
}

// The values a conjunct reads, through its own value or the ops its stage computes for it, that
// a conjunct before it or the code before the filter computes
void DBStagePlanner::collectCrossings(Block* block, Operation::operand_range columns) {
    _crossings.clear();
    _crossingIndices.clear();

    const auto noteRead = [this, block, &columns](Value value, size_t position) {
        const bool constant = ::db::yieldsConstantColumn(value, _constantColumns);
        const bool filtered = llvm::is_contained(columns, value);
        if (constant || filtered) {
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
            if (!isComputedAt(op, position)) {
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
        const size_t first = getFirstCrossedBoundary(crossing);
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

    for (size_t index = 0; index < _stageStarts.size(); index++) {
        const size_t begin = _stageStarts[index];
        const size_t end = index + 1 < _stageStarts.size() ? _stageStarts[index + 1] : _conjuncts.size();

        if (begin > 0) {
            for (size_t position = begin; position < end; position++) {
                for (Operation* const op : getCone(_conjuncts[position])) {
                    if (isComputedAt(op, position)) {
                        stages._moved.push_back(op);
                    }
                }
            }
        }

        if (end < _conjuncts.size()) {
            for (const StageCrossing& crossing : _crossings) {
                if (crossesBoundary(crossing, end)) {
                    stages._carried.push_back(crossing._value);
                }
            }
        }

        stages._stageEnds.push_back(StageEnd {end, stages._moved.size(), stages._carried.size()});
    }
}

double DBStagePlanner::rowCostOf(Operation* op) {
    if (::db::yieldsConstantColumn(op->getResult(0), _constantColumns)) {
        return 0;
    } else if (RowCostOpInterface costed = dyn_cast<RowCostOpInterface>(op)) {
        return costed.getRowCost();
    }

    return 1;
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

bool DBStagePlanner::isComputedAt(Operation* op, size_t position) const {
    size_t stage = 0;
    return findStageOf(op, stage) && stage == position;
}

size_t DBStagePlanner::getFirstCrossedBoundary(const StageCrossing& crossing) {
    return std::max<size_t>(1, crossing._crossesFrom);
}

bool DBStagePlanner::crossesBoundary(const StageCrossing& crossing, size_t boundary) {
    return getFirstCrossedBoundary(crossing) <= boundary && boundary <= crossing._lastRead;
}
