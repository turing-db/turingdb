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
void CartesianProductProcessor::fillOutput(Dataframe* left, Dataframe* right) {
    // Left DF is n x p dimensional
    const size_t n = left->getRowCount();
    const size_t p = left->size();

    // Right DF is m x q dimensional
    const size_t m = right->getRowCount();
    const size_t q = right->size();

    const size_t chunkSize = _ctxt->getChunkSize();

    Dataframe* oDF = _out.getDataframe();
    const size_t existingSize = oDF->getRowCount(); // XXX This just checks the first column
    bioassert(existingSize == _rowsWrittenThisCycle);

    const size_t rowsCanWrite = chunkSize - existingSize;
    bioassert(rowsCanWrite > 0);



    const size_t newSize = std::min(chunkSize, existingSize + n * m);

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

    fillOutput(lDF, rDF);

    finish();
    oPort->writeData();
}

