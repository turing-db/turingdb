#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

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

    void setLeftMemory(std::unique_ptr<Dataframe> leftMem) {
        _leftMemory = std::move(leftMem);
    }

    void setRightMemory(std::unique_ptr<Dataframe> rightMem) {
        _rightMemory = std::move(rightMem);
    }

private:
    enum class State {
        IDLE,
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

    // Depending on state, keeps track of how far in the input chunk/memory we have
    // processed
    size_t _lhsPtr {0};
    size_t _rhsPtr {0};

    size_t _rowsWrittenThisCycle {0};

    std::unique_ptr<Dataframe> _leftMemory;
    std::unique_ptr<Dataframe> _rightMemory;

    State _currentState {State::IDLE};

    void executeFromIdle();
    void executeFromImmediate();
    void executeFromLeftMem();
    void executeFromRightMem();

    void nextState();

    void fillOutput(Dataframe* left, Dataframe* right);
};

}

