#include "CartesianProductProcessor.h"

#include <algorithm>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "columns/ColumnDispatcher.h"
#include "dataframe/NamedColumn.h"

#include "FatalException.h"
#include "BioAssert.h"

using namespace db;

namespace {

void verifyAllColumnVectors(const Dataframe* df) {
    for (const NamedColumn* nCol : df->cols()) {
        const Column* col = nCol->getColumn();

        const ColumnKind::Code kind = col->getKind();
        const ContainerKind::Code containerKind = ColumnKind::extractContainerKind(kind);
        constexpr ContainerKind::Code ColumnVectorKind = ContainerKind::code<ColumnVector<size_t>>();

        if (containerKind != ColumnVectorKind) {
            std::string err =
                fmt::format("Attempt to calulate the CartesianProduct of a "
                            "Dataframe whose column is not a ColumnVector, but a {}.",
                            col->getTypeName());
            throw FatalException(std::move(err));
        }
    }
}

void verifyRectangular(const Dataframe* df) {
    const size_t rowCount = df->getLogicalRowCount();

    const bool rectangular = std::ranges::all_of(df->cols(), [rowCount](const NamedColumn* ncol) {
        return ncol->getColumn()->size() == rowCount;
    });

    if (!rectangular) {
        throw FatalException("CartesianProductProcessor was provided with "
                             "non-rectangular dataframe as input.");
    }
}

struct SetFromLeftCol {
public:
    SetFromLeftCol(Column* outCol, size_t rhsPtr, size_t lhsPtr, size_t rowPtr, size_t remainingSpace, size_t m, size_t n)
        : _outCol(outCol),
        _rhsPtr(rhsPtr),
        _lhsPtr(lhsPtr),
        _rowPtr(rowPtr),
        _remainingSpace(remainingSpace),
        _m(m),
        _n(n)
    {
    }

    template <typename T>
    void operator()(const ColumnVector<T>* lhsCol) {
        auto* outCol = dynamic_cast<ColumnVector<T>*>(_outCol);

        std::vector<T>& outRaw = outCol->getRaw();

        // If we were halfway through writing tuples for a left hand row, try and finish
        if (_rhsPtr != 0) {
            // Write as many as we need, or as many as we can
            const size_t needToWrite = _m - _rhsPtr;
            const size_t canWrite = std::min(_remainingSpace, needToWrite);

            const auto startIt = begin(outRaw) + _rowPtr;
            const auto endIt = startIt + canWrite;

            const T& val = lhsCol->at(_lhsPtr);

            std::fill(startIt, endIt, val);

            // Reduce the space we have left to right
            _remainingSpace -= canWrite;
            _rhsPtr += canWrite;
            _rowPtr += canWrite;
            // If we wrote all `m` rows for this LHS row, then reset,
            // otherwise increment
            if (canWrite == needToWrite) { // If we wrote all we needed
                _lhsPtr++;               // We now need to write the next LHS
                _rhsPtr = 0;             // And start from the first RHS
            }
        }

        if (_lhsPtr == _n + 1) { // We have written all rows from LHS
            return;
        }

        if (_remainingSpace == 0) { // We have ran out of space
            return;
        }

        // Work out how for how many rows in LHS can we write all tuples with RHS for
        // Each row in LHS needs m rows to fit all tuples: one for for each row on RHS
        const size_t rowsLeftToWrite = _n - _lhsPtr;
        const size_t numCompleteLhsRowsCanWrite = std::min(rowsLeftToWrite, _remainingSpace / _m);
        const bool canWriteAll = rowsLeftToWrite == numCompleteLhsRowsCanWrite;
        const bool canWriteLeftovers = _remainingSpace % _m != 0;

        for (size_t i = 0; i < numCompleteLhsRowsCanWrite; i++) {
            const T& currentLhsElement = lhsCol->at(_lhsPtr);

            const auto startIt = begin(outRaw) + _rowPtr;
            const auto endIt = startIt + _m; // We know we can fit m rows here

            std::fill(startIt, endIt, currentLhsElement);

            _lhsPtr++;
            _rhsPtr = 0;

            _rowPtr += _m;

            _remainingSpace -= _m;
        }

        if (canWriteAll || !canWriteLeftovers) {
            return;
        }

        bioassert(_remainingSpace < _m, "Not enough remaining space");
        bioassert(_rhsPtr == 0, "ourRhsPtr must be 0");

        const T& lhsElement = lhsCol->at(_lhsPtr);
        const auto startIt = begin(outRaw) + _rowPtr;
        const auto endIt = startIt + _remainingSpace;
        bioassert(endIt == end(outRaw), "Invalid endIt iterator");

        std::fill(startIt, endIt, lhsElement);
    }

private:
    Column* _outCol {nullptr};

    size_t _rhsPtr {0};
    size_t _lhsPtr {0};
    size_t _rowPtr {0};
    
    size_t _remainingSpace {0};

    size_t _m {0};
    size_t _n {0};
};

}

CartesianProductProcessor* CartesianProductProcessor::create(PipelineV2* pipeline) {
    auto* processor = new CartesianProductProcessor();

    {
        PipelineInputPort* lhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_lhs.setPort(lhsInput);
        processor->addInput(lhsInput);
        lhsInput->setNeedsData(false);
    }

    {
        PipelineInputPort* rhsInput = PipelineInputPort::create(pipeline, processor);
        processor->_rhs.setPort(rhsInput);
        processor->addInput(rhsInput);
        rhsInput->setNeedsData(false);
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

    markAsPrepared();
}

void CartesianProductProcessor::reset() {
    markAsReset();
}

void CartesianProductProcessor::nextState() {
    _rowsWrittenThisState = 0;
    _lhsPtr = 0;
    _rhsPtr = 0;

    switch (_currentState) {
        case State::INIT: {
            _currentState = State::IMMEDIATE_PORTS;
        }
        break;
        case State::IMMEDIATE_PORTS: {
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

void CartesianProductProcessor::setFromLeftColumn(Dataframe* left,
                                                  Dataframe* right,
                                                  size_t colIdx,
                                                  size_t fromRow,
                                                  size_t spaceAvailable) {
    size_t remainingSpace = spaceAvailable;

    const size_t n = left->getLogicalRowCount();
    const size_t m = right->getLogicalRowCount();

    Dataframe* oDF = _out.getDataframe();
    Column* outCol = oDF->cols().at(colIdx)->getColumn();

    Column* leftColumnErased = left->cols().at(colIdx)->getColumn();

    // NOTE: Functor takes @ref _rhsPtr, _lhsPtr by value, and doesn't modify global state
    SetFromLeftCol setter(outCol, _rhsPtr, _lhsPtr, fromRow, remainingSpace, m, n);

    using Types = CartesianProductKinds;
    using Dispatcher = ColumnSingleDispatcher<Types::Allowed, SetFromLeftCol, Types::Excluded>;

    Dispatcher::dispatch(leftColumnErased, setter);
}

void CartesianProductProcessor::copyFromRightColumn(Dataframe* left,
                                                    Dataframe* right,
                                                    size_t colIdx,
                                                    size_t fromRow,
                                                    size_t spaceAvailable) {
    size_t remainingSpace = spaceAvailable;
    size_t ourRowPtr = fromRow;

    const size_t n = left->getLogicalRowCount();
    const size_t m = right->getLogicalRowCount();
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

        bioassert(ourRhsPtr == 0, "ourRhsPtr must be zero");
        for (size_t i = 0; i < numCompleteLhsRowsCanWrite; i++) {
            const auto rStart = begin(rhsRaw) + ourRhsPtr;
            const auto rEnd = rStart + m; // We know we can fit m rows here
            bioassert(rEnd == end(rhsRaw), "rEnd is not valid");
            const auto outStart = begin(outRaw) + ourRowPtr;

            std::copy(rStart, rEnd, outStart);

            ourLhsPtr++;
            ourRhsPtr = 0;

            ourRowPtr += m;

            remainingSpace -= m;
        }

        if (canWriteAll || !canWriteLeftovers) {
            return;
        }

        bioassert(remainingSpace < m, "not enough remaining space");
        bioassert(ourRhsPtr == 0, "ourRhsPtr must be zero");

        const auto rStart = begin(rhsRaw) + ourRhsPtr;
        const auto rEnd = rStart + remainingSpace;
        bioassert(rEnd != end(rhsRaw), "invalid rEnd");

        const auto outStart = begin(outRaw) + ourRowPtr;
        bioassert(outStart + remainingSpace == end(outRaw), "invalid outStart");

        std::copy(rStart, rEnd, outStart);

        ourRhsPtr += remainingSpace;
        ourRowPtr += remainingSpace;
        remainingSpace -= remainingSpace;

        bioassert(remainingSpace == 0, "we must not have remainingSpace here");
    });
}

size_t CartesianProductProcessor::fillOutput(Dataframe* left, Dataframe* right) {
    // Left DF is n x p dimensional
    const size_t n = left->getLogicalRowCount();
    const size_t p = left->size();

    // Right DF is m x q dimensional
    const size_t m = right->getLogicalRowCount();
    const size_t q = right->size();

    const size_t chunkSize = _ctxt->getChunkSize();

    Dataframe* oDF = _out.getDataframe();
    // XXX: Below there is an implicit assumption that _rowsWrittenThisCycle is the number
    // of rows in the dataframe that contain meaningful data (i.e. should not be
    // overwritten). Because we resize the output DF on first call to @ref fillOutput, it
    // is not shrunk again after the data is read by the reader, so its size remains the
    // same, even though the data is safe to overwrite because the reader has marked the
    // port with @ref consume

    // Absolute max space we can use
    const size_t rowsCanWrite = chunkSize - _rowsWrittenThisCycle;
    // Number of rows we need to write based on how far we have progressed so far
    const size_t rowsNeedWrite = (m - _rhsPtr) + (n - _lhsPtr - 1) * (m);
    const size_t rowsShouldWrite = std::min(rowsCanWrite, rowsNeedWrite);
    bioassert(rowsCanWrite > 0, "no rows to write");

    const size_t newSize = std::min(chunkSize, _rowsWrittenThisCycle + rowsShouldWrite);

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
    // Account for the space used filling the remainder of the row currently half-worked
    // by a non-zero @ref _rhsPtr
    size_t remainingSpaceOnEntry = rowsShouldWrite;
    const size_t rowsNeededforEntryLhsPtr = m - _rhsPtr;
    const size_t rowsUsedForEntryLhsPtr =
        std::min(remainingSpaceOnEntry, rowsNeededforEntryLhsPtr);

    // If we completed finished writing the leftovers, we move to the next LHS row and
    // reset our RHS
    if (rowsUsedForEntryLhsPtr == rowsNeededforEntryLhsPtr) {
        _lhsPtr++;
        _rhsPtr = 0;
    } else { // Else we are still on the same LHS row, but progressed with RHS
        _rhsPtr += rowsUsedForEntryLhsPtr;
    }
    // Check if we wrote any more than this
    remainingSpaceOnEntry -= rowsUsedForEntryLhsPtr;
    if (remainingSpaceOnEntry == 0) {
        _out.getPort()->writeData();
        return rowsShouldWrite;
    }

    // See for how many rows on the LHS could we write all m required tuples
    const size_t remainingCompleteLhsRows = (remainingSpaceOnEntry / m);
    _lhsPtr += remainingCompleteLhsRows;

    // Check if we wrote any more than this
    remainingSpaceOnEntry -= remainingCompleteLhsRows * m;
    if (remainingSpaceOnEntry == 0) {
        _out.getPort()->writeData();
        return rowsShouldWrite;
    }

    // Check if we had any more space to partially complete a row from LHS
    const size_t partialRhsRowsWritten = remainingSpaceOnEntry;
    _rhsPtr += partialRhsRowsWritten;

    _out.getPort()->writeData();

    return rowsShouldWrite;
}

void CartesianProductProcessor::emitFromPorts() {
    // If either port is empty, then skip this stage
    if (!_lhs.getPort()->hasData() || !_rhs.getPort()->hasData()) {
        nextState();
        return;
    }

    Dataframe* lDF = _lhs.getDataframe();
    Dataframe* rDF = _rhs.getDataframe();

    if (lDF->getLogicalRowCount() == 0 || rDF->getLogicalRowCount() == 0) {
        nextState();
        return;
    }

    bioassert(_rowsWrittenThisCycle <= _ctxt->getChunkSize(),
              "more rows to write than the size of a chunk");

    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;

    if (remainingSpace == 0) {
        // No space, have not written what we need: stay in same state
        return;
    }

    const size_t rowsWritten = fillOutput(lDF, rDF); // Fill from immediate ports
    _rowsWrittenSinceLastFinished += rowsWritten;
    _rowsWrittenThisCycle += rowsWritten;

    const size_t rowsNeededToWrite = lDF->getLogicalRowCount() * rDF->getLogicalRowCount();

    if (_rowsWrittenSinceLastFinished != rowsNeededToWrite) {
        // We could not write all we needed -> return, remaining in same state
        return;
    }

    nextState(); // Sets state to @ref RIGHT_MEMORY
}

void CartesianProductProcessor::emitFromRightMemory() {
    // No left port => nothing to do
    if (!_lhs.getPort()->hasData()) {
        nextState();
        return;
    }

    // Nothing in memory => nothing to do
    if (_rightMemory.getLogicalRowCount() == 0) {
        nextState();
        return;
    }

    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;
    if (remainingSpace == 0) {
        // No space, have not written what we need: stay in same state
        return;
    }

    Dataframe* lDf = _lhs.getDataframe();
    Dataframe* rDf = &_rightMemory;

    const size_t rowsNeedToWrite = lDf->getLogicalRowCount() * rDf->getLogicalRowCount();
    
    // Emit port(L) x port-memory(R)
    const size_t rowsWritten = fillOutput(lDf, rDf);

    _rowsWrittenSinceLastFinished += rowsWritten;
    _rowsWrittenThisState += rowsWritten;
    _rowsWrittenThisCycle += rowsWritten;

    if (_rowsWrittenThisState != rowsNeedToWrite) {
        // No space, have not written what we need: stay in same state
        return;
    }

    nextState(); // Set state to @ref LEFT_MEMORY
}

void CartesianProductProcessor::emitFromLeftMemory() {
    // No right port => nothing to do
    if (!_rhs.getPort()->hasData()) {
        // No right memory: nothing to do
        nextState();
        return;
    }

    // Nothing in memory => nothing to do
    if (_leftMemory.getLogicalRowCount() == 0) {
        nextState();
        return;
    }

    const size_t remainingSpace = _ctxt->getChunkSize() - _rowsWrittenThisCycle;
    if (remainingSpace == 0) {
        // No space, have not written what we need: stay in same state
        return;
    }

    Dataframe* lDf = &_leftMemory;
    Dataframe* rDf = _rhs.getDataframe();

    const size_t rowsNeedToWrite = lDf->getLogicalRowCount() * rDf->getLogicalRowCount();

    // Emit port-memory(L) x port(R)
    const size_t rowsWritten = fillOutput(lDf, rDf);

    _rowsWrittenSinceLastFinished += rowsWritten;
    _rowsWrittenThisState += rowsWritten;
    _rowsWrittenThisCycle += rowsWritten;

    if (_rowsWrittenThisState != rowsNeedToWrite) {
        // No space, have not written what we need: stay in same state
        return;
    }

    nextState(); // Set state to @ref RESET
}

void CartesianProductProcessor::init() {
    verifyAllColumnVectors(_lhs.getDataframe());
    verifyAllColumnVectors(_rhs.getDataframe());
    verifyAllColumnVectors(_out.getDataframe());
    verifyRectangular(_lhs.getDataframe());
    verifyRectangular(_rhs.getDataframe());

    const size_t n = _lhs.getPort()->hasData() ? _lhs.getDataframe()->getLogicalRowCount() : 0;
    const size_t m = _rhs.getPort()->hasData() ? _rhs.getDataframe()->getLogicalRowCount() : 0;

    // LHS x RHS
    const size_t numRowsFromInputProd = n * m;
    // LHS x port-memory(R)
    const size_t numRowsFromRightMem = n * _rightMemory.getLogicalRowCount();
    // port-memory(L) x RHS
    const size_t numRowsFromLeftMem = _leftMemory.getLogicalRowCount() * m;

    _rowsToWriteBeforeFinished =
        numRowsFromInputProd + numRowsFromRightMem + numRowsFromLeftMem;

    nextState(); // Sets state to @ref IMMEDIATE
}

void CartesianProductProcessor::execute() {
    PipelineInputPort* leftPort = _lhs.getPort();
    PipelineInputPort* rightPort = _rhs.getPort();

    // We start with our output port being empty, and not having written any rows
    _rowsWrittenThisCycle = 0;

    if (_currentState == State::INIT) {
        init();
    }

    // Emit L x R
    if (_currentState == State::IMMEDIATE_PORTS) {
        emitFromPorts();
    }

    // Emit L x MEM(R)
    if (_currentState == State::RIGHT_MEMORY) {
        emitFromRightMemory();
    }

    // Emit MEM(L) x R
    if (_currentState == State::LEFT_MEMORY) {
        emitFromLeftMemory();
    }

    if (_rowsWrittenSinceLastFinished == _rowsToWriteBeforeFinished) {
        // Memorise the new chunks
        if (leftPort->hasData()) {
            _leftMemory.append(_lhs.getDataframe());
            leftPort->consume();
        }

        if (rightPort->hasData()) {
            _rightMemory.append(_rhs.getDataframe());
            rightPort->consume();
        }

        // Edge case: if we didn't need to write any rows, @ref fillOutput would not have
        // been called, and neither would @ref writeData. Call it explicitly here, but
        // only in the case where we are not to recieve any more inputs (i.e. both ports
        // closed), so that the following processor evaluates @ref hasData() to true, and
        // can therefore execute.
        const bool inputsClosed = leftPort->isClosed() && rightPort->isClosed();
        const bool nothingToWrite = _rowsToWriteBeforeFinished == 0;
        if (nothingToWrite && inputsClosed) {
            _out.getDataframe()->clear();
            _out.getPort()->writeData();
        }

        finish();
        _rowsWrittenSinceLastFinished = 0;
    }
}
