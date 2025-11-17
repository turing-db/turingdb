#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"
#include "dataframe/Dataframe.h"

namespace db {

class Dataframe;

}

namespace db::v2 {

class PipelineV2;

class CartesianProductProcessor final : public Processor {
public:
    static CartesianProductProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockInputInterface& leftHandSide() { return _lhs; }
    PipelineBlockInputInterface& rightHandSide() { return _rhs; }
    PipelineBlockOutputInterface& output() { return _out; }

    Dataframe& leftMemory() { return _leftMemory; }
    Dataframe& rightMemory() { return _rightMemory; }

private:
    enum class State {
        INIT,
        IMMEDIATE,
        RIGHT_MEMORY,
        LEFT_MEMORY,

        STATE_SPACE_SIZE
    };

    CartesianProductProcessor();
    ~CartesianProductProcessor() final;

    PipelineBlockInputInterface _lhs;
    PipelineBlockInputInterface _rhs;
    PipelineBlockOutputInterface _out;

    // In statate
    // - IMMEDIATE : tracks the index of the rows in left and right input ports that we next
    //               need to emit tuples from
    // - RIGHT(LEFT)_MEMORY : tracks the index of the rows in left (right) input ports and
    //                        right (left) memory that we next need to emit tuples from
    size_t _lhsPtr {0};
    size_t _rhsPtr {0};

    // A "cycle" is defined by a single call to @ref execute
    size_t _rowsWrittenThisCycle {0};
    size_t _rowsWrittenSinceLastFinished {0};
    size_t _rowsToWriteBeforeFinished {0};
    size_t _rowsWrittenThisState {0};

    Dataframe _leftMemory;
    Dataframe _rightMemory;

    State _currentState {State::INIT};

    void executeFromIdle();
    void executeFromImmediate();
    void executeFromLeftMem();
    void executeFromRightMem();

    void nextState();

    void init();

    size_t fillOutput(Dataframe* left, Dataframe* right);
    void setFromLeftColumn(Dataframe* left, Dataframe* right, size_t colIdx,
                           size_t fromRow, size_t spaceAvailable);
    void copyFromRightColumn(Dataframe* left, Dataframe* right, size_t colIdx,
                             size_t fromRow, size_t spaceAvailable);
};

}

