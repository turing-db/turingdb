#pragma once

#include <stddef.h>
#include <algorithm>

namespace db {

// Runtime state of one LIMIT: the remaining row budget and how many rows the
// current innermost step should emit. nl.limit resets it, nl.limit_update is the
// sole mutator (called once per innermost step before nl.output), and the loops
// and nl.output only read it. emitThisStep is what output emits; remaining is
// what the loops test to early-exit.
class NLLimitState {
public:
    void reset(size_t budget) {
        _remaining = budget;
        _emitThisStep = 0;
    }

    size_t getRemaining() const { return _remaining; }
    size_t getEmitThisStep() const { return _emitThisStep; }

    // Called once per innermost step by nl.limit_update, before nl.output: emit
    // min(rowsAvailable, remaining) this step and charge it against the budget.
    void update(size_t rowsAvailable) {
        _emitThisStep = std::min(rowsAvailable, _remaining);
        _remaining -= _emitThisStep;
    }

private:
    size_t _remaining {0};
    size_t _emitThisStep {0};
};

// Runtime state of one SKIP: the remaining rows still to drop, and - for the
// current innermost step - how many of its rows to drop off the front
// (skipThisStep, the offset into the chunk) and how many survive (emitThisStep).
// nl.skip resets it, nl.skip_update is the sole mutator (called once per innermost
// step before the truncate), and nl.skip_truncate only reads it. The skip sibling
// of NLLimitState - but it never gates a loop, since every row past the dropped
// prefix must still be produced.
class NLSkipState {
public:
    void reset(size_t toSkip) {
        _remaining = toSkip;
        _skipThisStep = 0;
        _emitThisStep = 0;
    }

    size_t getSkipThisStep() const { return _skipThisStep; }
    size_t getEmitThisStep() const { return _emitThisStep; }

    // Called once per innermost step by nl.skip_update, before nl.skip_truncate:
    // drop min(rowsAvailable, remaining) rows off the front of this step, emit the
    // rest, and charge the dropped count against the budget.
    void update(size_t rowsAvailable) {
        _skipThisStep = std::min(rowsAvailable, _remaining);
        _remaining -= _skipThisStep;
        _emitThisStep = rowsAvailable - _skipThisStep;
    }

private:
    size_t _remaining {0};
    size_t _skipThisStep {0};
    size_t _emitThisStep {0};
};

}
