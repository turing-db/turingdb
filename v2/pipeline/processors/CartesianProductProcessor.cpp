#include "CartesianProductProcessor.h"

#include <memory>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"
#include "FatalException.h"
#include "columns/ColumnSwitch.h"

using namespace db::v2;

CartesianProductProcessor::CartesianProductProcessor() = default;

CartesianProductProcessor::~CartesianProductProcessor() = default;

CartesianProductProcessor* CartesianProductProcessor::create(PipelineV2* pipeline) {
    auto* processor = new CartesianProductProcessor();

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

    {
        processor->_leftMemory = std::make_unique<Dataframe>();
        processor->_rightMemory = std::make_unique<Dataframe>();
    }

    processor->postCreate(pipeline);
    return processor;
}

void CartesianProductProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    _lhsPtr = 0;
    _rhsPtr = 0;

    markAsPrepared();
}

void CartesianProductProcessor::reset() {
    // TODO: Clear memory, reset ptrs, state
    markAsReset();
}

void CartesianProductProcessor::nextState() {
    _rowsWrittenThisState = 0;
    _finishedThisState = false;
    _lhsPtr = 0;
    _rhsPtr = 0;

    switch (_currentState) {
        case State::INIT: {
            _currentState = State::IMMEDIATE;
        }
        break;
        case State::IMMEDIATE: {
            _currentState = State::RIGHT_MEMORY;
        }
        break;

        case State::RIGHT_MEMORY: {
            _currentState = State::LEFT_MEMORY;
        }
        break;

        case State::LEFT_MEMORY : {
            _currentState = State::INIT;
        }
        break;
        case State::STATE_SPACE_SIZE: {
            throw FatalException("CartesianProduct Processor in invalid state.");
        }
        break;
    }
}

// Assumes that the entire column is writable
void CartesianProductProcessor::setFromLeftColumn(Dataframe* left, Dataframe* right,
                                              size_t colIdx, size_t fromRow,
                                              size_t spaceAvailable) {
    size_t remainingSpace = spaceAvailable;
    size_t ourRowPtr = fromRow;

    const size_t n = left->getRowCount();
    const size_t m = right->getRowCount();

    // Keep local copies, because each column needs to reuse the global state
    size_t ourLhsPtr = _lhsPtr;
    size_t ourRhsPtr = _rhsPtr;

    Dataframe* oDF = _out.getDataframe();
    Column* outColumnErased = oDF->cols().at(colIdx)->getColumn();

    Column* leftColumnErased = left->cols().at(colIdx)->getColumn();

    dispatchColumnVector(leftColumnErased, [&](auto* lhsCol) {
        auto* outCol = static_cast<decltype(lhsCol)>(outColumnErased);
        auto& outRaw = outCol->getRaw();

        // If we were halfway through writing tuples for a left hand row, try and finish
        if (ourRhsPtr != 0) {
            // Write as many as we need, or as many as we can
            const size_t needToWrite = m - ourRhsPtr;
            const size_t canWrite = std::min(remainingSpace, needToWrite);

            const auto startIt = begin(outRaw) + ourRowPtr;
            const auto endIt = startIt + canWrite;

            const auto& val = lhsCol->at(ourLhsPtr);

            std::fill(startIt, endIt, val);

            // Reduce the space we have left to right
            remainingSpace -= canWrite;
            ourRowPtr += canWrite;
            ourRhsPtr += canWrite;
            // If we wrote all `m` rows for this LHS row, then reset, otherwise increment
            if (canWrite == needToWrite) { // If we wrote all we needed
                ourLhsPtr++; // We now need to write the next LHS
                ourRhsPtr = 0; // And start from the first RHS
            }
        }

        if (ourLhsPtr == n + 1) { // We have written all rows from LHS
            return;
        }

        if (remainingSpace == 0) { // We have ran out of space
            return;
        }

        // Work out how for how many rows in LHS can we write all tuples with RHS for
        // Each row in LHS needs m rows to fit all tuples: one for for each row on RHS
        const size_t rowsLeftToWrite = n - ourLhsPtr;
        const size_t numCompleteLhsRowsCanWrite = std::min(rowsLeftToWrite, remainingSpace / m);
        const bool canWriteAll = rowsLeftToWrite == numCompleteLhsRowsCanWrite;
        const bool canWriteLeftovers = remainingSpace % m != 0;

        for (; ourLhsPtr < numCompleteLhsRowsCanWrite; ourLhsPtr++) {
            const auto& currentLhsElement = lhsCol->at(ourLhsPtr);

            const auto startIt = begin(outRaw) + ourRowPtr;
            const auto endIt = startIt + m; // We know we can fit m rows here

            std::fill(startIt, endIt, currentLhsElement);

            ourRowPtr += m;
            ourRhsPtr = 0;

            remainingSpace -= m;
        }

        if (canWriteAll || !canWriteLeftovers) { // We wrote everything we need
            return;
        }

        bioassert(remainingSpace < m);
        bioassert(ourRhsPtr == 0);

        const auto& lhsElement = lhsCol->at(ourLhsPtr);
        const auto startIt = begin(outRaw) + ourRowPtr;
        const auto endIt = startIt + remainingSpace;
        bioassert(endIt == end(outRaw));

        std::fill(startIt, endIt, lhsElement);
    });
}

void CartesianProductProcessor::copyFromRightColumn(Dataframe* left, Dataframe* right,
                                                size_t colIdx, size_t fromRow,
                                                size_t spaceAvailable) {
    size_t remainingSpace = spaceAvailable;
    size_t ourRowPtr = fromRow;

    const size_t n = left->getRowCount();
    const size_t m = right->getRowCount();
    const size_t p = left->size();

    size_t ourLhsPtr = _lhsPtr;
    size_t ourRhsPtr = _rhsPtr;

    Dataframe* oDF = _out.getDataframe();
    Column* outColumnErased = oDF->cols().at(p + colIdx)->getColumn();

    Column* rightColumnErased = right->cols().at(colIdx)->getColumn();

    dispatchColumnVector(rightColumnErased, [&](auto* rhsCol) {
        auto* outCol = static_cast<decltype(rhsCol)>(outColumnErased);
        auto& outRaw = outCol->getRaw();

        const auto& rhsRaw = rhsCol->getRaw();
        // If we were halfway through writing tuples for a left hand row, try and finish
        if (ourRhsPtr != 0) {
            const size_t needToWrite = m - ourRhsPtr;
            const size_t canWrite = std::min(remainingSpace, needToWrite);

            // Copy as much of the column as we can
            const auto rStart = begin(rhsRaw) + ourRhsPtr;
            const auto rEnd = rStart + canWrite;
            const auto outStart = begin(outRaw) + ourRowPtr;

            std::copy(rStart, rEnd, outStart);

            remainingSpace -= canWrite;
            ourRowPtr += canWrite;
            ourRhsPtr += canWrite;
            // If we copied all the remainder of the column, we start again on RHS
            if (canWrite == needToWrite) {
                ourLhsPtr++;
                ourRhsPtr = 0;
            }
        }

        if (ourLhsPtr == n + 1) {
            return;
        }

        if (remainingSpace == 0) {
            return;
        }

        const size_t rowsLeftToWrite = n - ourLhsPtr;
        const size_t numCompleteLhsRowsCanWrite =
            std::min(rowsLeftToWrite, remainingSpace / m);
        const bool canWriteAll = rowsLeftToWrite == numCompleteLhsRowsCanWrite;
        const bool canWriteLeftovers = remainingSpace % m != 0;

        bioassert(ourRhsPtr == 0);
        for (; ourLhsPtr < numCompleteLhsRowsCanWrite; ourLhsPtr++) {
            const auto rStart = begin(rhsRaw) + ourRhsPtr;
            const auto rEnd = rStart + m; // We know we can fit m rows here
            bioassert(rEnd == end(rhsRaw));
            const auto outStart = begin(outRaw) + ourRowPtr;

            std::copy(rStart, rEnd, outStart);

            ourRhsPtr = 0;
            ourRowPtr += m;

            remainingSpace -= m;
        }

        if (canWriteAll || !canWriteLeftovers) {
            return;
        }

        bioassert(remainingSpace < m);
        bioassert(ourRhsPtr == 0);

        const auto rStart = begin(rhsRaw) + ourRhsPtr;
        const auto rEnd = rStart + remainingSpace;
        bioassert(rEnd != end(rhsRaw));

        const auto outStart = begin(outRaw) + ourRowPtr;
        bioassert(outStart + remainingSpace == end(outRaw));

        std::copy(rStart, rEnd, outStart);

        ourRhsPtr += remainingSpace;
        ourRowPtr += remainingSpace;
        remainingSpace -= remainingSpace;

        bioassert(remainingSpace == 0);
    });
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
    const size_t rowsNeedWrite = (m - _rhsPtr) + (n - _lhsPtr - 1) * (m);
    const size_t rowsShouldWrite = std::min(rowsCanWrite, rowsNeedWrite);
    bioassert(rowsCanWrite > 0);

    const size_t newSize = std::min(chunkSize, rowsShouldWrite);

    // Resize all columns in the output to be the correct size, then we can just copy in
    for (NamedColumn* col : oDF->cols()) {
        dispatchColumnVector(col->getColumn(),
            [&](auto* columnVector) { columnVector->resize(newSize); }
        );
    }

    // Copy over LHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < p; colPtr++) {
        setFromLeftColumn(left, right, colPtr, _rowsWrittenThisCycle, rowsShouldWrite);
    }

    // Copy over RHS columns to output, column-wise
    for (size_t colPtr = 0; colPtr < q; colPtr++) {
        copyFromRightColumn(left, right, colPtr, _rowsWrittenThisCycle, rowsShouldWrite);
    }

    // Below computes the next _lhsPtr and _rhsPtrs
    size_t remainingSpaceOnEntry = rowsShouldWrite;
    const size_t rowsNeededforEntryLhsPtr = m - _rhsPtr;
    const size_t rowsUsedForEntryLhsPtr =
        std::min(remainingSpaceOnEntry, rowsNeededforEntryLhsPtr);

    if (rowsUsedForEntryLhsPtr == rowsNeededforEntryLhsPtr) {
        _lhsPtr++;
        _rhsPtr = 0;
    }
    remainingSpaceOnEntry -= rowsUsedForEntryLhsPtr;
    if (remainingSpaceOnEntry == 0) {
        _out.getPort()->writeData();
        return rowsShouldWrite;
    }

    const size_t remainingCompleteLhsRows = (remainingSpaceOnEntry / m);
    _lhsPtr += remainingCompleteLhsRows;

    remainingSpaceOnEntry -= remainingCompleteLhsRows * m;
    if (remainingSpaceOnEntry == 0) {
        _out.getPort()->writeData();
        return rowsShouldWrite;
    }

    const size_t partialRhsRowsWritten = remainingSpaceOnEntry;
    _rhsPtr+= partialRhsRowsWritten;

    _out.getPort()->writeData();

    return rowsShouldWrite;
}

void CartesianProductProcessor::executeFromImmediate() {
    // If either port is empty, then skip this stage
    if (!_lhs.getPort()->hasData() || !_rhs.getPort()->hasData()) {
        nextState();
        return;
    }

    bioassert(_rowsWrittenThisCycle <= _ctxt->getChunkSize());

    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;

    if (remainingSpace == 0) {
        // No space, have not written what we need: stay in same state
        return;
    }

    Dataframe* lDF = _lhs.getDataframe();
    Dataframe* rDF = _rhs.getDataframe();

    const size_t rowsWritten = fillOutput(lDF, rDF); // Fill from immediate ports
    _rowsWrittenSinceLastFinished += rowsWritten;
    // n * m - rows we already wrote
    const size_t rowsNeededToWrite = lDF->getRowCount() * rDF->getRowCount();

    if (_rowsWrittenSinceLastFinished != rowsNeededToWrite) {
        // We could not write all we needed -> return, remaining in same state
        return;
    }

    nextState(); // Sets state to @ref RIGHT_MEMORY
}

void CartesianProductProcessor::executeFromRightMem() {
    // No left port => nothing to do
    if (!_lhs.getPort()->hasData()) {
        nextState();
        return;
    }

    // Nothing in memory => nothing to do
    if (_rightMemory->getRowCount() == 0) {
        nextState();
        return;
    }


    Dataframe* lDf = _lhs.getDataframe();
    Dataframe* rDf = _rightMemory.get();

    const size_t rowsNeedToWrite = lDf->getRowCount() * rDf->getRowCount();
    
    // Emit port(L) x port-memory(R)
    const size_t rowsWritten = fillOutput(lDf, rDf);

    _rowsWrittenSinceLastFinished += rowsWritten;
    _rowsWrittenThisState += rowsWritten;

    if (_rowsWrittenThisState != rowsNeedToWrite) {
        // No space, have not written what we need: stay in same state
        return;
    }

    nextState(); // Set state to @ref LEFT_MEMORY
}

void CartesianProductProcessor::executeFromLeftMem() {
    // No right port => nothing to do
    if (!_rhs.getPort()->hasData()) {
        // No right memory: nothing to do
        nextState();
        return;
    }

    // Nothing in memory => nothing to do
    if (_leftMemory->getRowCount() == 0) {
        nextState();
        return;
    }

    Dataframe* lDf = _leftMemory.get();
    Dataframe* rDf = _rhs.getDataframe();

    const size_t rowsNeedToWrite = lDf->getRowCount() * rDf->getRowCount();

    // Emit port-memory(L) x port(R)
    const size_t rowsWritten = fillOutput(lDf, rDf);

    _rowsWrittenSinceLastFinished += rowsWritten;
    _rowsWrittenThisState += rowsWritten;

    if (_rowsWrittenThisState != rowsNeedToWrite) {
        // No space, have not written what we need: stay in same state
        return;
    }

    nextState(); // Set state to @ref RESET
}

void CartesianProductProcessor::init() {
    bioassert(_rhsPtr == 0);
    bioassert(_lhsPtr == 0);
    bioassert(_rowsWrittenThisCycle == 0);

    // Add to memory
    // _leftMemory->append(_lhs.getDataframe());
    // _rightMemory->append(_rhs.getDataframe());

    const size_t n = _lhs.getPort()->hasData() ? _lhs.getDataframe()->getRowCount() : 0;
    const size_t m = _rhs.getPort()->hasData() ? _rhs.getDataframe()->getRowCount() : 0;

    // LHS x RHS
    const size_t numRowsFromInputProd = n * m;
    // LHS x port-memory(R)
    const size_t numRowsFromRightMem = n * _rightMemory->getRowCount();
    // port-memory(L) x RHS
    const size_t numRowsFromLeftMem = _leftMemory->getRowCount() * m;

    _rowsToWriteBeforeFinished =
        numRowsFromInputProd + numRowsFromRightMem + numRowsFromLeftMem;

    nextState(); // Sets state to @ref IMMEDIATE
}

void CartesianProductProcessor::execute() {
    // We start with our output port being empty, and not having written any rows
    _rowsWrittenThisCycle = 0;

    if (_currentState == State::INIT) {
        init();
    }

    if (_currentState == State::IMMEDIATE) {
        executeFromImmediate();
    }

    if (_currentState == State::RIGHT_MEMORY) {
        executeFromRightMem();
    }

    if (_currentState == State::LEFT_MEMORY) {
        executeFromLeftMem();
    }

    if (_rowsWrittenSinceLastFinished == _rowsToWriteBeforeFinished) {
        finish();
        _lhs.getPort()->consume();
        _rhs.getPort()->consume();
        _rowsWrittenSinceLastFinished = 0;
    }
}

