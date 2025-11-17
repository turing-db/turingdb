#pragma once

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

    std::string_view describe() const final {
        return "CartesianProductProcessor";
    }

    PipelineBlockInputInterface& leftHandSide() { return _lhs; }
    PipelineBlockInputInterface& rightHandSide() { return _rhs; }
    PipelineBlockOutputInterface& output() { return _out; }

    Dataframe& leftMemory() { return _leftMemory; }
    Dataframe& rightMemory() { return _rightMemory; }

private:
    enum class State {
        INIT,
        IMMEDIATE_PORTS,
        RIGHT_MEMORY,
        LEFT_MEMORY,

        STATE_SPACE_SIZE
    };

    CartesianProductProcessor() = default;
    ~CartesianProductProcessor() final = default;

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

    void emitFromPorts();
    void emitFromLeftMemory();
    void emitFromRightMemory();

    void nextState();

    void init();

    /**
     * @brief Emits as many rows of the Cartesian Product of the dataframes @param left
     * and @param right into the dataframe in the @ref _out interface.
     * @detail Emits starting from, and including, the row indexed by @ref _lhsPtr in
     * @param left, and emits starting from, and including, the row indexed by @ref
     * _rhsPtr in @param right.
     * @returns The number of rows successfully written.
     */
    size_t fillOutput(Dataframe* left, Dataframe* right);

    /**
     * @brief Sets subspans of rows in @ref _out->getDataframe(), starting with index
     * @param fromRow, to be elements of column @ref colIdx of @param left, starting with
     * the element of @ref _lhsPtr.
     * @detail The number of rows set for the first element is dependent on the space
     * remaining in the output column, the number of rows in @param right, and - for the
     * first element to be set only - the position of @param _rhsPtr.
     * @warn Does not perform bound checking on @param spaceAvailable
     */
    void setFromLeftColumn(Dataframe* left,
                           Dataframe* right,
                           size_t colIdx,
                           size_t fromRow,
                           size_t spaceAvailable);

    /**
     * @brief Copies (possibly subspans) of columns from @param right into @ref
     * _out.getDataframe() until @param spaceAvailable rows are written.
     * @detail Starts with row @ref _rhsPtr in @param right.
     * @warn Does not perform bound checking on @param spaceAvailable
     */
    void copyFromRightColumn(Dataframe* left,
                             Dataframe* right,
                             size_t colIdx,
                             size_t fromRow,
                             size_t spaceAvailable);
};

}

