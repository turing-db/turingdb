#include "CartesianProductProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "columns/ColumnSwitch.h"

using namespace db::v2;

CartesianProductProcessor::CartesianProductProcessor()
{
}

CartesianProductProcessor::~CartesianProductProcessor()
{
}

CartesianProductProcessor* CartesianProductProcessor::create(PipelineV2* pipeline) {
    CartesianProductProcessor* processor = new CartesianProductProcessor();

    {
        PipelineInputPort* lhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_lhs.setPort(lhsInput);
        processor->addInput(lhsInput);
    }

    {
        PipelineInputPort* rhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_rhs.setPort(rhsInput);
        processor->addInput(rhsInput);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
        processor->_out.setPort(output);
        processor->addOutput(output);
    }

    processor->postCreate(pipeline);
    return processor;
}

void CartesianProductProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    _lhsPtr = 0;
    _rhsPtr = 0;

    // Should be initialised in @ref PipelineBuilder
    bioassert(_leftMemory);
    bioassert(_rightMemory);

    markAsPrepared();
}

void CartesianProductProcessor::reset() {
    // TODO: Clear memory, reset ptrs, state
    markAsReset();
}

void CartesianProductProcessor::nextState() {
    switch (_currentState) {
        case State::IDLE : {
            bioassert(_lhsPtr == 0);
            bioassert(_rhsPtr == 0);
            _currentState = State::IMMEDIATE;
        }
        break;

        case State::IMMEDIATE: {
            // TODO: Assert preconditions
            _currentState = State::RIGHT_MEMORY;
            // TODO: Reset counters if needed
        }
        break;

        case State::RIGHT_MEMORY: {
            // TODO: Assert preconditions
            _currentState = State::LEFT_MEMORY;
            // TODO: Reset counters if needed
        }
        break;

        case State::LEFT_MEMORY : {
            // TODO: Assert preconditions
            _currentState = State::IDLE;
            // TODO: Reset counters if needed
        }
        break;
        default:
            throw FatalException("CartesianProduct Processor in invalid state.");
    }
}

/**
 * @brief Computes the @param k rows of the Cartesian product of @param left and @param
 * right, and stores them in @ref _out's Dataframe.
 */
void CartesianProductProcessor::emitKRows(Dataframe* left, Dataframe* right, size_t k) {
    bioassert(k <= _ctxt->getChunkSize());
    bioassert(_rowsWrittenThisCycle + k <= _ctxt->getChunkSize());

    if (k == 0) {
        return;
    }

    [[maybe_unused]] size_t chunkSize = _ctxt->getChunkSize();

    Dataframe* oDF = _out.getDataframe();
    size_t existingSize = oDF->getRowCount(); // XXX This just checks the first column
    bioassert(existingSize == _rowsWrittenThisCycle);

    // Resize all columns in the output to be the correct size, then we can just copy in
    for (NamedColumn* col : oDF->cols()) {
        dispatchColumnVector(col->getColumn(),
            [&](auto* columnVector) { columnVector->resize(existingSize + k); }
        );
    }

    // Left DF is n x p dimensional
    const size_t n = left->getRowCount();
    const size_t p = left->size();

    // Right DF is m x q dimensional
    const size_t m = right->getRowCount();
    const size_t q = right->size();

    // Copy over LHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < p; colPtr++) {
        dispatchColumnVector(left->cols()[colPtr]->getColumn(), [&](auto* lhsColumn) {
            auto* outCol = static_cast<decltype(lhsColumn)>(oDF->cols()[colPtr]->getColumn());

            auto& outRaw = outCol->getRaw();

            // Each row from LHS needs to appear m times consecutively, so we can use a
            // memset-esque approach to materialise each row from LHS m times
            // consecutively in output
            for (size_t lhsRow = 0; lhsRow < n; lhsRow++) {
                const auto& currentLSHElement = lhsColumn->at(lhsRow);
                auto startIt = outRaw.begin() + (lhsRow * m);
                std::fill(startIt, startIt + m, currentLSHElement);
            }
        });
    }

    // Copy over RHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < q; colPtr++) {
        dispatchColumnVector(right->cols()[colPtr]->getColumn(), [&](auto* rhsColumn) {
            auto* outCol = static_cast<decltype(rhsColumn)>(oDF->cols()[p + colPtr]->getColumn());

            const auto& rhsRaw = rhsColumn->getRaw();
            auto& outRaw = outCol->getRaw();

            // All rows from RHS needed for each row in LHS; for each unique row on LHS
            // copy the entire RHS column over starting from that unique LHS row's first
            // index
            for (size_t lhsRow = 0; lhsRow < n; lhsRow++) {
                auto startIt = outRaw.begin() + lhsRow * m;
                std::copy(rhsRaw.begin(), rhsRaw.end(), startIt);
            }
        });
    }
}

void CartesianProductProcessor::executeFromIdle() {
    bioassert(_currentState == State::IDLE);
    // We should not be in the middle of reading/writing anything if in IDLE
    bioassert(_lhsPtr == 0);
    bioassert(_rhsPtr == 0);
    bioassert(_rowsWrittenThisCycle == 0);

    nextState(); // Sets state to IMMEDIATE

    executeFromImmediate();
}

void CartesianProductProcessor::executeFromImmediate() {
    bioassert(_currentState == State::IMMEDIATE);

    bioassert(_rowsWrittenThisCycle <= _ctxt->getChunkSize());
    size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;

    if (remainingSpace == 0) {
        return;
    }

}

void CartesianProductProcessor::execute() {
    // We start with our output port being empty, and not have written any rows
    _rowsWrittenThisCycle = 0;

    PipelineInputPort* lPort = _lhs.getPort();
    PipelineInputPort* rPort = _rhs.getPort();
    PipelineOutputPort* oPort = _out.getPort();

    Dataframe* lDF = _lhs.getDataframe();
    lPort->consume();
    Dataframe* rDF = _rhs.getDataframe();
    rPort->consume();
    [[maybe_unused]] Dataframe* oDF = _out.getDataframe();

    // Left DF is n x p dimensional
    const size_t n = lDF->getRowCount();
    [[maybe_unused]] const size_t p = lDF->size();

    // Right DF is m x q dimensional
    const size_t m = rDF->getRowCount();
    [[maybe_unused]] const size_t q = rDF->size();

    msgbioassert(n * m <= _ctxt->getChunkSize(),
                 "Cartesian Product is only supported in the strongly bounded case "
                 "(output size <= CHUNK_SIZE).");

    emitKRows(lDF, rDF, n * m);

    finish();
    oPort->writeData();
}

