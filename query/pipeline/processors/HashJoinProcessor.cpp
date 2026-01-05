#include "HashJoinProcessor.h"

#include "ExecutionContext.h"
#include "RowStore.h"
#include "columns/ColumnDispatcher.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "BioAssert.h"

using namespace db;

namespace {

template <typename T>
    requires std::is_base_of_v<Column, T>
void fillOutputColumn(T* outputColumn,
                      const T* inputColumn,
                      const size_t rowIndex,
                      const size_t offset,
                      const size_t rowsToCopy) {
    bioassert(outputColumn->size() >= offset + rowsToCopy,
              "rows to copy do not fit in output column");
    std::fill_n(outputColumn->begin() + offset,
                rowsToCopy,
                (*inputColumn)[rowIndex]);
}

template <typename Key, typename Value>
auto findInMap(std::unordered_map<Key, Value>& map, Column* col, size_t index) {
    const auto it = map.find((*static_cast<ColumnVector<Key>*>(col))[index]);
    return it;
}

template <typename Key>
std::vector<RowOffset>& getMap(std::unordered_map<Key, std::vector<RowOffset>>& map,
                               Column* col,
                               size_t index) {
    return map[(*static_cast<const ColumnVector<Key>*>(col))[index]];
}

}

std::string HashJoinProcessor::describe() const {
    return fmt::format("HashJoinProcessor @={}", fmt::ptr(this));
}

HashJoinProcessor::HashJoinProcessor(const ColumnTag leftJoinKey,
                                     const ColumnTag rightJoinKey)
    : _leftJoinKey(leftJoinKey),
    _rightJoinKey(rightJoinKey)
{
}

HashJoinProcessor* HashJoinProcessor::create(PipelineV2* pipeline,
                                             const ColumnTag leftJoinKey,
                                             const ColumnTag rightJoinKey) {
    HashJoinProcessor* hashJoin = new HashJoinProcessor(leftJoinKey, rightJoinKey);

    PipelineInputPort* leftInput = PipelineInputPort::create(pipeline, hashJoin);
    leftInput->setNeedsData(false);
    hashJoin->_leftInput.setPort(leftInput);
    hashJoin->addInput(leftInput);

    PipelineInputPort* rightInput = PipelineInputPort::create(pipeline, hashJoin);
    rightInput->setNeedsData(false);
    hashJoin->_rightInput.setPort(rightInput);
    hashJoin->addInput(rightInput);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, hashJoin);
    hashJoin->_output.setPort(output);
    hashJoin->addOutput(output);

    hashJoin->postCreate(pipeline);

    return hashJoin;
}

void HashJoinProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const auto* leftDf = _leftInput.getDataframe();
    const auto* outDf = _output.getDataframe();

    // The stored rows do not contain the join column
    _leftRowLen = leftDf->size() - 1;

    //We have already calculated what columns of the right
    //input are to be skipped when constructing the output dataframe
    //so we can get the length of the right output row by subtracting
    //the reserved space of the left columns and the join column from
    //the size of the output dataframe
    _rightRowLen = outDf->size() - _leftRowLen - 1;

    markAsPrepared();
}
void HashJoinProcessor::reset() {
    markAsReset();
}

void HashJoinProcessor::execute() {
    size_t rowsRemaining = _ctxt->getChunkSize();
    size_t totalRowsInserted = 0;

    const Dataframe* leftDf = _leftInput.getDataframe();
    const Dataframe* rightDf = _rightInput.getDataframe();

    Dataframe* outDf = _output.getDataframe();

    if (_rowOffsetState.hasRowOffsets()) {
        // resize the output columns to fit the new rows we are inserting
        const size_t rowOffsetsRemaining = _rowOffsetState.numRemainingOffsets();
        const size_t newOutputSize = std::min(_ctxt->getChunkSize(), rowOffsetsRemaining);

        for (const auto& namedCol : outDf->cols()) {
            dispatchColumnVector(namedCol->getColumn(),
                                 [&](auto* col) {
                                     auto* colVector = static_cast<decltype(col)>(col);
                                     colVector->resize(newOutputSize);
                                 });
        }

        if (_rowOffsetState._df == rightDf) {
            flushRightStream(rowsRemaining,
                             totalRowsInserted);
        } else {
            flushLeftStream(rowsRemaining,
                            totalRowsInserted);
        }

        // If we have a valid copy state
        // this means that we haven't finished
        // processing the rowOffset vector after
        // filling up an output chunk
        if (_rowOffsetState.hasRowOffsets()) {
            _output.getPort()->writeData();
            _hasWritten = true;
            return;
        }

        if (rowsRemaining == 0) {
            // If there are no rows remaining in our output chunk we can write the output
            _output.getPort()->writeData();
            _hasWritten = true;
            return;
        }
    }

    if (_leftInput.getPort()->hasData()) {
        auto* leftCol = leftDf->getColumn(_leftJoinKey)->getColumn();
        size_t totalSizeIncrease = 0;

        // calculate how many hash hits we get for the input so we can allocate once
        for (size_t i = _leftInputIdx; i < leftCol->size(); ++i) {
            if (const auto it = findInMap(_rightMap, leftCol, i); it != _rightMap.end()) {
                const auto& rows = it->second;
                totalSizeIncrease += rows.size();
            }
        }

        if (totalSizeIncrease) {
            const size_t newOutputSize = std::min(_ctxt->getChunkSize(),
                                                  totalRowsInserted + totalSizeIncrease);
            for (const auto* namedCol : outDf->cols()) {
                dispatchColumnVector(namedCol->getColumn(),
                                     [&](auto* col) {
                                         auto* colVector = static_cast<decltype(col)>(col);
                                         colVector->resize(newOutputSize);
                                     });
            }
        }

        processLeftStream(rowsRemaining, totalRowsInserted);

        if (rowsRemaining == 0) {
            // If there are no rows remaining in our output chunk we can write the output
            _output.getPort()->writeData();
            _hasWritten = true;

            // if we aren't paused mid-row vector (no valid _copyState)
            // and we don't have any more columns to read we can consume
            // the leftInput (the case where we finish reading the left
            // input stream and the output fits perfectly into a chunk)
            if (!_rowOffsetState.hasRowOffsets() && _leftInputIdx == leftDf->getRowCount()) {
                _leftInputIdx = 0;
                _leftInput.getPort()->consume();
            }

            return;
        }

        // At this point we know the left stream has been fully processed so
        // we can reset the index and consume the data
        _leftInputIdx = 0;
        _leftInput.getPort()->consume();
    }

    if (_rightInput.getPort()->hasData()) {
        auto* rightCol = rightDf->getColumn(_rightJoinKey)->getColumn();
        size_t totalSizeIncrease = 0;
        // calculate total size of new additions from hashes on the right column and allocate once.-
        for (size_t i = _rightInputIdx; i < rightCol->size(); ++i) {
            if (const auto it = findInMap(_leftMap, rightCol, i); it != _rightMap.end()) {
                const auto& rows = it->second;
                totalSizeIncrease += rows.size();
            }
        }

        if (totalSizeIncrease) {
            const size_t newOutputSize = std::min(_ctxt->getChunkSize(),
                                                  totalRowsInserted + totalSizeIncrease);
            for (const auto* namedCol : outDf->cols()) {
                dispatchColumnVector(namedCol->getColumn(),
                                     [&](auto* col) {
                                         auto* colVector = static_cast<decltype(col)>(col);
                                         colVector->resize(newOutputSize);
                                     });
            }
        }

        processRightStream(rowsRemaining, totalRowsInserted);

        // if we aren't paused mid-row vector (no _copyState)
        // and we don't have any more columns to read we can consume
        // the rightInput
        if (!_rowOffsetState.hasRowOffsets() && _rightInputIdx == rightDf->getRowCount()) {
            _rightInputIdx = 0;
            _rightInput.getPort()->consume();
        }
    }

    // If we have written to the output but have not completed a whole chunk
    if (totalRowsInserted > 0) {
        _output.getPort()->writeData();
        _hasWritten = true;
    }

    if (_leftInput.getPort()->isClosed() && _rightInput.getPort()->isClosed()) {
        if (!_hasWritten) {
            _output.getPort()->writeData();
        }
    }

    finish();
}

void HashJoinProcessor::processLeftStream(size_t& rowsRemaining,
                                          size_t& totalRowsInserted) {
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    auto* leftCol = leftDf->getColumn(_leftJoinKey)->getColumn();

    for (; _leftInputIdx < leftCol->size(); ++_leftInputIdx) {
        const auto it = findInMap(_rightMap, leftCol, _leftInputIdx);
        if (it != _rightMap.end()) {
            const auto& rows = it->second;
            const auto& cols = leftDf->cols();
            const auto& outCols = outDf->cols();

            const size_t rowsToCopy = std::min(rows.size(), rowsRemaining);

            // If we can't write anymore outputrows
            // but still haven't written all the rows
            // for the hash
            if (rowsToCopy != rows.size()) {
                // initialise the copystate here
                _rowOffsetState.initialise(leftDf, &rows, rowsToCopy);
            }

            rowsRemaining -= rowsToCopy;

            for (size_t j = 0; j < leftDf->size(); ++j) {
                if (cols[j]->getTag() == _leftJoinKey) {
                    continue;
                }

                auto* inputColumn = cols[j]->getColumn();
                auto* outputColumn = outCols[j]->getColumn();

                dispatchColumnVector(outputColumn, [&](auto* col) {
                    auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
                    const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                    fillOutputColumn(typedOutCol,
                                     typedInCol,
                                     _leftInputIdx,
                                     totalRowsInserted,
                                     rowsToCopy);
                });
            }

            // Copy the join column to the last output column
            auto* inputColumn = leftDf->getColumn(_leftJoinKey)->getColumn();
            auto* outputColumn = outCols.back()->getColumn();

            dispatchColumnVector(outputColumn, [&](auto* col) {
                auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
                const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                fillOutputColumn(typedOutCol,
                                 typedInCol,
                                 _leftInputIdx,
                                 totalRowsInserted,
                                 rowsToCopy);
            });

            // copy over the stored rows:
            // here we calculate the starting index of the output column that we are
            // inserting the row into  as _leftRowLen as we we want to copy the left input of
            // the join into the first columns of the output.
            for (size_t k = 0; k < rowsToCopy; ++k) {
                _store.copyRow(outDf,
                               _leftRowLen,
                               totalRowsInserted + k,
                               _rightRowLen,
                               rows[k]);
            }
            totalRowsInserted += rowsToCopy;
        }

        // Create a hash table entry for the input
        auto& offsetVec = getMap(_leftMap,
                                 leftCol,
                                 _leftInputIdx);
        offsetVec.emplace_back(_store.insertRow(leftDf,
                                                _leftJoinKey,
                                                _leftRowLen,
                                                _leftInputIdx));

        // If we can't write anymore rows to the output chunk
        // but still haven't gone through all the input rows
        if (rowsRemaining == 0) {
            break;
        }
    }

    // Increment the input index to start processing the input
    // from next row on the next cycle. We don't do this if we
    // still have to finish processing a retreived vector of row offsets,
    // or if we have fully processed the input
    if (!_rowOffsetState.hasRowOffsets() && _leftInputIdx != leftCol->size()) {
        _leftInputIdx += 1;
    }
}

void HashJoinProcessor::processRightStream(size_t& rowsRemaining,
                                           size_t& totalRowsInserted) {
    const Dataframe* rightDf = _rightInput.getDataframe();
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    auto* rightCol = rightDf->getColumn(_rightJoinKey)->getColumn();

    for (; _rightInputIdx < rightDf->getRowCount(); ++_rightInputIdx) {
        const auto it = findInMap(_leftMap, rightCol, _rightInputIdx);
        if (it != _leftMap.end()) {
            const auto& rows = it->second;
            const auto& cols = rightDf->cols();
            const auto& outCols = outDf->cols();

            const size_t rowsToCopy = std::min(rows.size(), rowsRemaining);

            // If we can't write anymore outputrows
            // but still haven't written all the rows
            // for the hash
            if (rowsToCopy != rows.size()) {
                // initialise the copystate here
                _rowOffsetState.initialise(rightDf, &rows, rowsToCopy);
            }

            rowsRemaining -= rowsToCopy;

            size_t columnOffset = _leftRowLen;
            for (size_t j = 0; j < rightDf->size(); ++j) {
                //Skip Columns present in the left input and the join key
                if (leftDf->getColumn(cols[j]->getTag()) != nullptr || 
                    cols[j]->getTag() == _rightJoinKey) {
                    continue;
                }

                // We insert into the output while leaving a gap for the left rows
                auto* inputColumn = cols[j]->getColumn();
                auto* outputColumn = outCols[columnOffset]->getColumn();
                dispatchColumnVector(outputColumn, [&](auto* col) {
                    auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
                    const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                    fillOutputColumn(typedOutCol,
                                     typedInCol,
                                     _rightInputIdx,
                                     totalRowsInserted,
                                     rowsToCopy);
                });
                ++columnOffset;
            }

            // Copy the join column to the last output column
            auto* inputColumn = rightDf->getColumn(_rightJoinKey)->getColumn();
            auto* outputColumn = outCols.back()->getColumn();

            dispatchColumnVector(outputColumn, [&](auto* col) {
                auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
                const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                fillOutputColumn(typedOutCol,
                                 typedInCol,
                                 _rightInputIdx,
                                 totalRowsInserted,
                                 rowsToCopy);
            });

            for (size_t k = 0; k < rowsToCopy; ++k) {
                _store.copyRow(outDf,
                               0, // The left rows should be inserted from col index 0
                               totalRowsInserted + k,
                               _leftRowLen,
                               rows[k]);
            }
            totalRowsInserted += rowsToCopy;
        }

        auto& offsetVec = getMap(_rightMap,
                                 rightCol,
                                 _rightInputIdx);

        offsetVec.emplace_back(_store.insertRow(rightDf,
                                                leftDf,
                                                _rightJoinKey,
                                                _rightRowLen,
                                                _rightInputIdx));

        // break if we don't have any rows remaining to write
        if (rowsRemaining == 0) {
            break;
        }
    }

    // if we don't need to finish of copying a row offset vector
    // we can start from the next index
    if (!_rowOffsetState.hasRowOffsets() && _rightInputIdx != rightCol->size()) {
        _rightInputIdx += 1;
    }
}

void HashJoinProcessor::flushRightStream(size_t& rowsRemaining,
                                         size_t& totalRowsInserted) {
    const Dataframe* rightDf = _rightInput.getDataframe();
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    const auto& rows = *_rowOffsetState._offsetVec;
    const size_t rowOffsetIdx = _rowOffsetState._rowOffsetIdx;

    const auto& outCols = outDf->cols();
    const auto& cols = rightDf->cols();

    const size_t rowsToCopy = std::min(rows.size() - rowOffsetIdx, rowsRemaining);

    for (size_t j = 0; j < rightDf->size(); ++j) {
        //Skip Columns present in the left input and the join key
        if (leftDf->getColumn(cols[j]->getTag()) != nullptr || 
            cols[j]->getTag() == _rightJoinKey) {
            continue;
        }

        auto* inputColumn = cols[j]->getColumn();
        auto* outputColumn = outCols[_leftRowLen + j]->getColumn();
        dispatchColumnVector(outputColumn, [&](auto* col) {
            auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
            const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
            fillOutputColumn(typedOutCol,
                             typedInCol,
                             _rightInputIdx,
                             totalRowsInserted,
                             rowsToCopy);
        });
    }

    // Copy the join column to the last output column
    auto* inputColumn = rightDf->getColumn(_rightJoinKey)->getColumn();
    auto* outputColumn = outCols.back()->getColumn();

    dispatchColumnVector(outputColumn, [&](auto* col) {
        auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
        const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
        fillOutputColumn(typedOutCol,
                         typedInCol,
                         _rightInputIdx,
                         totalRowsInserted,
                         rowsToCopy);
    });

    for (size_t k = rowOffsetIdx; k < rowOffsetIdx + rowsToCopy; ++k) {
        _store.copyRow(outDf,
                       0,
                       k - rowOffsetIdx, // helps us keep track of which row of the output df we are writing to
                       _leftRowLen,
                       rows[k]);
    }

    totalRowsInserted += rowsToCopy;

    // If we haven't finished reading the offset vector
    if (rows.size() != rowOffsetIdx + rowsToCopy) {
        // increment the rowOffset to the new offset (keeping state valid)
        _rowOffsetState.incrementRowOffsetIdx(rowsToCopy);
    } else {
        _rowOffsetState.reset();
        _rightInputIdx += 1;
    }

    rowsRemaining -= rowsToCopy;
}

void HashJoinProcessor::flushLeftStream(size_t& rowsRemaining,
                                        size_t& totalRowsInserted) {
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    const auto& rows = *_rowOffsetState._offsetVec;
    const size_t rowOffsetIdx = _rowOffsetState._rowOffsetIdx;

    const auto& cols = leftDf->cols();
    const auto& outCols = outDf->cols();

    const size_t rowsToCopy = std::min(rows.size() - rowOffsetIdx, rowsRemaining);

    for (size_t j = 0; j < leftDf->size(); ++j) {
        if (cols[j]->getTag() == _leftJoinKey) {
            continue;
        }

        auto* inputColumn = cols[j]->getColumn();
        auto* outputColumn = outCols[j]->getColumn();
        dispatchColumnVector(outputColumn, [&](auto* col) {
            auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
            const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
            fillOutputColumn(typedOutCol,
                             typedInCol,
                             _leftInputIdx,
                             totalRowsInserted,
                             rowsToCopy);
        });
    }
    // Copy the join column to the last output column
    auto* inputColumn = leftDf->getColumn(_leftJoinKey)->getColumn();
    auto* outputColumn = outCols.back()->getColumn();

    dispatchColumnVector(outputColumn, [&](auto* col) {
        auto* typedOutCol = static_cast<decltype(col)>(outputColumn);
        const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
        fillOutputColumn(typedOutCol,
                         typedInCol,
                         _leftInputIdx,
                         totalRowsInserted,
                         rowsToCopy);
    });

    for (size_t k = rowOffsetIdx; k < rowOffsetIdx + rowsToCopy; ++k) {
        _store.copyRow(outDf,
                       _leftRowLen,
                       k - rowOffsetIdx, // helps us keep track of which row of the output we are writing to
                       _rightRowLen,
                       rows[k]);
    }

    totalRowsInserted += rowsToCopy;

    // If we haven't finished reading the offset vector
    if (rows.size() != rowOffsetIdx + rowsToCopy) {
        // increment the rowOffset to the new offset
        _rowOffsetState.incrementRowOffsetIdx(rowsToCopy);
    } else {
        _rowOffsetState.reset();
        _leftInputIdx += 1;
    }

    rowsRemaining -= rowsToCopy;
}
