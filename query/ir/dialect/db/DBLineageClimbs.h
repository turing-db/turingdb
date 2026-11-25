#pragma once

#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "mlir/IR/Operation.h"
#include "mlir/IR/Value.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"

namespace mlir::db {

using ColumnEquality = std::pair<Value, Value>;

// Every equality of two node columns the filter requires: it keeps only the rows where they
// agree, so below it anything reading one of them reads the other just as well. A filter
// whose mask is a conjunction requires each of its conjuncts, equalities among them.
void collectColumnEqualities(FilterOp filter, llvm::SmallVectorImpl<ColumnEquality>& equalities);

// One step of the climb from a column to the column its variable was bound at. It crosses a
// row op where the op is not a filter: it builds, orders or groups the rows rather than only
// dropping some
struct LineageStep {
    Value _next;
    bool _isAnchor {false};
    bool _crossesRowOp {false};
};

// Sets _isAnchor where @param column is where its variable was bound: a node source, or an op
// carrying the rows in flight that makes the column. Sets _next to the column the climb goes
// on from, and neither where the climb cannot pass the op defining it
void stepTowardLineageAnchor(Value column, LineageStep& step);

// The climbs of one pass run, each built from the climb one step up. A push_down_filters push
// moves a predicate onto the op making its anchors, which changes no anchor and crosses no row
// op, so a climb stays true across pushes; its equalities are kept as their sides' anchors,
// which a push never erases. A rewrite that replaces an anchor has to clear() them.
class LineageClimbs {
public:
    Value anchorOf(Value column) {
        computeClimb(column);
        return _climbs.at(column)._anchor;
    }

    Value climb(Value column, bool& crossedRowOp, llvm::SmallVectorImpl<ColumnEquality>& anchorEqualities) {
        computeClimb(column);

        const Climb& climb = _climbs.at(column);
        crossedRowOp = crossedRowOp || climb._crossedRowOp;

        if (climb._equalities != NO_LIST) {
            llvm::append_range(anchorEqualities, _equalityLists[climb._equalities]);
        }

        return climb._anchor;
    }

    void forget(Operation* op) {
        for (const Value result : op->getResults()) {
            _climbs.erase(result);
        }
    }

    void clear() {
        _climbs.clear();
        _equalityLists.clear();
    }

private:
    static constexpr size_t NO_LIST = SIZE_MAX;

    struct Climb {
        Value _anchor;
        bool _crossedRowOp {false};
        size_t _equalities {NO_LIST};
    };

    // A climb's distinct equalities, nearest filter first. A filter whose equalities the climb
    // one step up already holds shares that list, so a chain repeating one equality keeps one.
    llvm::DenseMap<Value, Climb> _climbs;
    std::vector<llvm::SmallVector<ColumnEquality, 2>> _equalityLists;

    void computeClimb(Value column) {
        llvm::SmallVector<Value> worklist {column};
        llvm::SmallVector<ColumnEquality, 4> filterEqualities;

        while (!worklist.empty()) {
            const Value value = worklist.back();
            if (_climbs.contains(value)) {
                worklist.pop_back();
                continue;
            }

            LineageStep step;
            stepTowardLineageAnchor(value, step);

            filterEqualities.clear();
            if (FilterOp filter = value.getDefiningOp<FilterOp>()) {
                collectColumnEqualities(filter, filterEqualities);
            }

            bool dependenciesPending = false;
            const auto requireClimb = [this, &worklist, &dependenciesPending](Value dependency) {
                if (dependency && !_climbs.contains(dependency)) {
                    worklist.push_back(dependency);
                    dependenciesPending = true;
                }
            };

            requireClimb(step._next);
            for (const ColumnEquality& equality : filterEqualities) {
                requireClimb(equality.first);
                requireClimb(equality.second);
            }

            if (dependenciesPending) {
                continue;
            }

            Climb climb;
            if (step._isAnchor) {
                climb._anchor = value;
            } else if (step._next) {
                const Climb next = _climbs.at(step._next);
                climb._anchor = next._anchor;
                climb._crossedRowOp = next._crossedRowOp || step._crossesRowOp;
                climb._equalities = next._equalities;

                for (const ColumnEquality& equality : llvm::reverse(filterEqualities)) {
                    const ColumnEquality anchors {_climbs.at(equality.first)._anchor, _climbs.at(equality.second)._anchor};
                    const bool relatesTwoAnchors = anchors.first && anchors.second && anchors.first != anchors.second;
                    if (!relatesTwoAnchors) {
                        continue;
                    }

                    llvm::SmallVector<ColumnEquality, 2> equalities {anchors};
                    if (climb._equalities != NO_LIST) {
                        const llvm::SmallVector<ColumnEquality, 2>& held = _equalityLists[climb._equalities];
                        if (llvm::is_contained(held, anchors)) {
                            continue;
                        }

                        llvm::append_range(equalities, held);
                    }

                    _equalityLists.push_back(equalities);
                    climb._equalities = _equalityLists.size() - 1;
                }
            }

            _climbs[value] = climb;
            worklist.pop_back();
        }
    }
};

}
