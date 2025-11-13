#include "CartesianProductProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "columns/ColumnSwitch.h"
#include "spdlog/spdlog.h"

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
            _currentState = State::RESET;
            // TODO: Reset counters if needed
        }
        break;
        case State::RESET: {
            resetState();
        }
        break;
        case State::STATE_SPACE_SIZE: {
            throw FatalException("CartesianProduct Processor in invalid state.");
        }
        break;
    }
}

void CartesianProductProcessor::resetState() {
    _lhsPtr = 0;
    _rhsPtr = 0;

    _rowsWrittenSinceLastFinished += _rowsWrittenThisCycle;
    _rowsWrittenThisCycle = 0;

    _currentState = State::IDLE;
}

/**
 * @brief Computes the @param k rows of the Cartesian product of @param left and @param
 * right, and stores them in @ref _out's Dataframe.
 */
size_t CartesianProductProcessor::fillOutput(Dataframe* left, Dataframe* right) {
    // Left DF is n x p dimensional
    const size_t n = left->getRowCount();
    const size_t p = left->size();

    // Right DF is m x q dimensional
    const size_t m = right->getRowCount();
    const size_t q = right->size();

    const size_t chunkSize = _ctxt->getChunkSize();

    Dataframe* oDF = _out.getDataframe();
    // XXX: Below there is an implicit assumption that _rowsWrittenThisCycle is the number
    // of rows in the dataframe that contain meaningful data (i.e. should not be
    // overwritten). Because we resize the output DF on first call to @ref fillOutput, it
    // is not shrunk again after the data is read by the reader, so its size remains the
    // same, even though the data is safe to overwrite because the reader has marked the
    // port with @ref consume

    // const size_t existingSize = oDF->getRowCount(); // XXX This just checks the first column

    const size_t rowsCanWrite = chunkSize - _rowsWrittenThisCycle;
    bioassert(rowsCanWrite > 0);

    const size_t newSize = std::min(chunkSize, _rowsWrittenThisCycle + n * m);

    // Resize all columns in the output to be the correct size, then we can just copy in
    for (NamedColumn* col : oDF->cols()) {
        dispatchColumnVector(col->getColumn(),
            [&](auto* columnVector) { columnVector->resize(newSize); }
        );
    }

    // Each row on LHS needs m rows allocated for it
    const size_t numUniqueLhsRowsCanWrite = std::min(n, rowsCanWrite / m);

    // Copy over LHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < p; colPtr++) {
        dispatchColumnVector(left->cols()[colPtr]->getColumn(), [&](auto* lhsColumn) {
            auto* outCol = static_cast<decltype(lhsColumn)>(oDF->cols()[colPtr]->getColumn());
            auto& outRaw = outCol->getRaw();

            // Write the rows from LHS that we can fit all m rows in output
            for (size_t lhsRow = _lhsPtr; lhsRow < numUniqueLhsRowsCanWrite; lhsRow++) {
                const auto& currentLhsElement = lhsColumn->at(lhsRow);
                const auto startIt = outRaw.begin() + (lhsRow * m);
                bioassert((lhsRow * m) + m <= outRaw.size());
                const auto endIt = startIt + m; // We know we can fit m rows here

                std::fill(startIt, endIt, currentLhsElement);
            }

            // If we didn't fit all n rows, and there is space left:
            if (numUniqueLhsRowsCanWrite < n) {
                // We have one more row that needs writing, but we cannot
                // fit all m rows, so we have to truncate it, and keep track of where
                // we truncated
                const auto& lhsElement = lhsColumn->at(numUniqueLhsRowsCanWrite);
                const auto startIt = outRaw.begin() + (numUniqueLhsRowsCanWrite * m);
                const auto endIt = outRaw.end();

                std::fill(startIt, endIt, lhsElement);
            }
        });
    }

    // Copy over RHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < q; colPtr++) {
        dispatchColumnVector(right->cols()[colPtr]->getColumn(), [&](auto* rhsColumn) {
            auto* outCol = static_cast<decltype(rhsColumn)>(oDF->cols()[p + colPtr]->getColumn());
            auto& outRaw = outCol->getRaw();

            const auto& rhsRaw = rhsColumn->getRaw();

            // We know we can fill each of these entirely with m entries
            for (size_t forLhsRow = _lhsPtr; forLhsRow < numUniqueLhsRowsCanWrite; forLhsRow++) {
                const auto rhsStart = rhsRaw.begin() + _rhsPtr;
                const auto rhsEnd = rhsStart + m;
                bioassert(rhsEnd == rhsRaw.end());
                const auto outStart = outRaw.begin() + (forLhsRow * m);

                std::copy(rhsStart, rhsEnd, outStart);
                // _rhsPtr = 0
            }
            // If we didn't fit all n rows, and there is space left:
            if (numUniqueLhsRowsCanWrite < n) {
                const auto spaceLeft = rowsCanWrite % m;
                const auto rhsStart = rhsRaw.begin() + _rhsPtr;
                const auto rhsEnd = rhsStart + spaceLeft;
                const auto outStart = outRaw.begin() + (numUniqueLhsRowsCanWrite * m);

                std::copy(rhsStart, rhsEnd, outStart);

            }
        });
    }

    if (numUniqueLhsRowsCanWrite == n) { // Wrote all pairs for all rows
        _lhsPtr += numUniqueLhsRowsCanWrite;
        _rhsPtr = 0;
    } else if (numUniqueLhsRowsCanWrite < n) { // Could not fit all rows/all pairs
        // Start next time from the first LHS row we didnt complete
        _lhsPtr = numUniqueLhsRowsCanWrite;
        // And start writing pairs from as far as we got
        _rhsPtr = rowsCanWrite % m;
    }

    const size_t rowsWritten = numUniqueLhsRowsCanWrite * m + rowsCanWrite % m;
    bioassert(rowsCanWrite == rowsWritten); // Sanity check

    _rowsWrittenThisCycle += rowsWritten;
    _rowsWrittenSinceLastFinished += rowsWritten;

    _out.getPort()->writeData();

    return rowsWritten;
}

void CartesianProductProcessor::executeFromIdle() {
    bioassert(_currentState == State::IDLE);
    // We should not be in the middle of reading/writing anything if in IDLE bioassert(_lhsPtr == 0);
    bioassert(_rhsPtr == 0);
    bioassert(_rowsWrittenThisCycle == 0);

    const size_t n = _lhs.getDataframe()->getRowCount();
    const size_t m = _rhs.getDataframe()->getRowCount();

    // LHS x RHS
    const size_t numRowsFromInputProd = n * m;
    // LHS x port-memory(R)
    const size_t numRowsFromRightMem = n * _rightMemory->getRowCount();
    // port-memory(L) x RHS
    const size_t numRowsFromLeftMem = _leftMemory->getRowCount() * m;

    _rowsToWriteBeforeFinished =
        numRowsFromInputProd + numRowsFromRightMem + numRowsFromLeftMem;

    nextState(); // Sets state to @ref IMMEDIATE

    executeFromImmediate();
}

void CartesianProductProcessor::executeFromImmediate() {
    bioassert(_currentState == State::IMMEDIATE);

    bioassert(_rowsWrittenThisCycle <= _ctxt->getChunkSize());

    {
        const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;

        if (remainingSpace == 0) {
            // No space, have not written what we need: stay in same state
            return;
        }
    }

    Dataframe* lDF = _lhs.getDataframe();
    Dataframe* rDF = _rhs.getDataframe();

    const size_t rowsWritten = fillOutput(lDF, rDF); // Fill from immediate ports
    // n * m - rows we already wrote
    const size_t rowsNeededToWrite =
        lDF->getRowCount() * rDF->getRowCount() - (_rowsToWriteBeforeFinished -_rowsWrittenSinceLastFinished);

    if (rowsWritten != rowsNeededToWrite) {
        spdlog::info("We wrote {} rows but we wanted {}", rowsWritten, rowsNeededToWrite);
        // We could not write all we needed -> return, remaining in same state
        return;
    }

    nextState(); // Sets state to @ref RIGHT_MEMORY
    executeFromRightMem();
}

void CartesianProductProcessor::executeFromRightMem() {
    if (_rightMemory->getRowCount() == 0) {
        // No right memory: nothing to do
        nextState();
        return;
    }
    
    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;
    if (remainingSpace == 0 ) {
        // No space, have not written what we need: stay in same state
        return;
    }

    // fillOutput(left input, right memory)
    // TODO: Check if we wrote all we need

    nextState(); // Set state to @ref LEFT_MEMORY
}

void CartesianProductProcessor::executeFromLeftMem() {
    if (_leftMemory->getRowCount() == 0) {
        // No right memory: nothing to do
        nextState();
        return;
    }

    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;
    if (remainingSpace == 0) {
        // No space, have not written what we need: stay in same state
        return;
    }

    // fillOutput(left memory, right input)
    // TODO: Check if we wrote all we need
     
    nextState(); // Set state to @ref RESET
}

void CartesianProductProcessor::execute() {
    // We start with our output port being empty, and not having written any rows
    _rowsWrittenThisCycle = 0;
    
    spdlog::info("CartProd::exec");
    spdlog::info("We are in state {}", (int)_currentState);
    spdlog::info("lhsPtr  = {}", _lhsPtr);
    spdlog::info("rhsPtr  = {}", _rhsPtr);

    switch (_currentState) {
        case State::IDLE: {
            executeFromIdle();
        }
        break;
        case State::IMMEDIATE: {
            executeFromImmediate();
        }
        break;
        case State::RIGHT_MEMORY: {
            executeFromRightMem();
        }
        break;
        case State::LEFT_MEMORY: {
            executeFromLeftMem();
        }
        break;
        case State::RESET: {
            resetState();
        }
        break;
        case State::STATE_SPACE_SIZE: {
            throw FatalException("Unknown state of CartesianProductProcessor");
        } break;
    }

    if (_rowsWrittenSinceLastFinished == _rowsToWriteBeforeFinished) {
        finish();
        _rowsWrittenSinceLastFinished = 0;
    }
}

