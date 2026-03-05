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

template <typename Key>
bool hasKey(Column* col, size_t index, bool isOptional) {
    if (!isOptional) {
        return true;
    }
    auto* typedCol = static_cast<ColumnVector<std::optional<Key>>*>(col);
    const auto& val = (*typedCol)[index];
    return val.has_value();
}

// Key is the column's value type (e.g. string_view). The map's key type
// may differ (e.g. std::string) so we accept the map via auto&.
template <typename Key>
auto findInMap(const auto& map, Column* col, size_t index, bool isOptional) {
    if (isOptional) {
        auto* typedCol = static_cast<ColumnVector<std::optional<Key>>*>(col);
        const auto& val = (*typedCol)[index];
        if (!val.has_value()) {
            return map.end();
        }
        return map.find(*val);
    }
    auto* typedCol = static_cast<ColumnVector<Key>*>(col);
    return map.find((*typedCol)[index]);
}

// Extracts the key from the column, converting string_view to std::string
// so the map owns the key data.
template <typename Key>
auto extractKey(Column* col, size_t index, bool isOptional) {
    if constexpr (std::is_same_v<Key, std::string_view>) {
        if (isOptional) {
            auto* typedCol = static_cast<ColumnVector<std::optional<Key>>*>(col);
            return std::string(*(*typedCol)[index]);
        } else {
            auto* typedCol = static_cast<const ColumnVector<Key>*>(col);
            return std::string((*typedCol)[index]);
        }
    } else {
        if (isOptional) {
            auto* typedCol = static_cast<ColumnVector<std::optional<Key>>*>(col);
            return *(*typedCol)[index];
        } else {
            auto* typedCol = static_cast<const ColumnVector<Key>*>(col);
            return (*typedCol)[index];
        }
    }
}

template <typename Key>
std::vector<RowOffset>& getMap(auto& map,
                               Column* col,
                               size_t index,
                               bool isOptional) {
    return map[extractKey<Key>(col, index, isOptional)];
}

}

template <typename Key>
std::string HashJoinProcessor<Key>::describe() const {
    return fmt::format("HashJoinProcessor @={}", fmt::ptr(this));
}

template <typename Key>
HashJoinProcessor<Key>::HashJoinProcessor(const ColumnTag leftJoinKey,
                                          const ColumnTag rightJoinKey)
    : _leftJoinKey(leftJoinKey),
    _rightJoinKey(rightJoinKey)
{
}

template <typename Key>
HashJoinProcessor<Key>* HashJoinProcessor<Key>::create(PipelineV2* pipeline,
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

template <typename Key>
void HashJoinProcessor<Key>::prepare(ExecutionContext* ctxt) {
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

    // Detect if the join key columns are optional (nullable)
    {
        auto* leftKeyCol = leftDf->getColumn(_leftJoinKey)->getColumn();
        const auto leftKind = leftKeyCol->getKind();
        _isLeftOptionalKey = (leftKind == ColumnVector<std::optional<Key>>::staticKind());
        bioassert(_isLeftOptionalKey || leftKind == ColumnVector<Key>::staticKind(),
                  "left join key column type does not match HashJoinProcessor key type");
    }

    {
        const auto* rightDf = _rightInput.getDataframe();
        auto* rightKeyCol = rightDf->getColumn(_rightJoinKey)->getColumn();
        const auto rightKind = rightKeyCol->getKind();
        _isRightOptionalKey = (rightKind == ColumnVector<std::optional<Key>>::staticKind());
        bioassert(_isRightOptionalKey || rightKind == ColumnVector<Key>::staticKind(),
                  "right join key column type does not match HashJoinProcessor key type");
    }

    markAsPrepared();
}

template <typename Key>
void HashJoinProcessor<Key>::reset() {
    _hasWritten = false;
    markAsReset();
}

template <typename Key>
void HashJoinProcessor<Key>::execute() {
    size_t rowsRemaining = _ctxt->getChunkSize();
    size_t totalRowsInserted = 0;

    const Dataframe* leftDf = _leftInput.getDataframe();
    const Dataframe* rightDf = _rightInput.getDataframe();

    Dataframe* outDf = _output.getDataframe();

    // Restart reading from a row store if we had paused due to filling
    // out a chunk in the previous cycle.
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

        // The case where we have don't have any rows left to copy from the rows store
        // and there is no space left in the output data frame (the entire generated output
        // fits perfectly into 1 chunk).
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
            if (const auto it = findInMap<Key>(_rightMap, leftCol, i, _isLeftOptionalKey); it != _rightMap.end()) {
                const auto& rows = it->second;
                totalSizeIncrease += rows.size();
            }
        }

        if (totalSizeIncrease) {
            // The output size of the df will be the number of rows we have already
            // added so far in the execution + the calculated total size increase we
            // have just calculated.
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
            if (!_rowOffsetState.hasRowOffsets() && _leftInputIdx == leftDf->getLogicalRowCount()) {
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
            if (const auto it = findInMap<Key>(_leftMap, rightCol, i, _isRightOptionalKey); it != _leftMap.end()) {
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
        if (!_rowOffsetState.hasRowOffsets() && _rightInputIdx == rightDf->getLogicalRowCount()) {
            _rightInputIdx = 0;
            _rightInput.getPort()->consume();
        }
    }

    // If we have written to the output but have not completed a whole chunk
    if (totalRowsInserted > 0) {
        _output.getPort()->writeData();
        _hasWritten = true;
    }

    // This covers the case when we have empty inputs to a processor. Writing
    // Data to the output port lets the pipeline cycle continue.
    if (_leftInput.getPort()->isClosed() && _rightInput.getPort()->isClosed()) {
        if (!_hasWritten) {
            _output.getPort()->writeData();
        }
    }

    //Only mark as finished if we have consumed all the inputs and do not have any
    //rows left to copy.
    if(!_rowOffsetState.hasRowOffsets() &&
        !_leftInput.getPort()->hasData() &&
        !_rightInput.getPort()->hasData()) {
        finish();
    }
}

template <typename Key>
void HashJoinProcessor<Key>::processLeftStream(size_t& rowsRemaining,
                                               size_t& totalRowsInserted) {
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    auto* leftCol = leftDf->getColumn(_leftJoinKey)->getColumn();

    for (; _leftInputIdx < leftCol->size(); ++_leftInputIdx) {
        // Skip null keys - NULL != NULL semantics
        if (!hasKey<Key>(leftCol, _leftInputIdx, _isLeftOptionalKey)) {
            continue;
        }

        const auto it = findInMap<Key>(_rightMap, leftCol, _leftInputIdx, _isLeftOptionalKey);
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
        auto& offsetVec = getMap<Key>(_leftMap,
                                     leftCol,
                                     _leftInputIdx,
                                     _isLeftOptionalKey);
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

template <typename Key>
void HashJoinProcessor<Key>::processRightStream(size_t& rowsRemaining,
                                                size_t& totalRowsInserted) {
    const Dataframe* rightDf = _rightInput.getDataframe();
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    auto* rightCol = rightDf->getColumn(_rightJoinKey)->getColumn();

    for (; _rightInputIdx < rightDf->getLogicalRowCount(); ++_rightInputIdx) {
        // Skip null keys - NULL != NULL semantics
        if (!hasKey<Key>(rightCol, _rightInputIdx, _isRightOptionalKey)) {
            continue;
        }

        const auto it = findInMap<Key>(_leftMap, rightCol, _rightInputIdx, _isRightOptionalKey);
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

        auto& offsetVec = getMap<Key>(_rightMap,
                                     rightCol,
                                     _rightInputIdx,
                                     _isRightOptionalKey);

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

template <typename Key>
void HashJoinProcessor<Key>::flushRightStream(size_t& rowsRemaining, size_t& totalRowsInserted) {
    const Dataframe* rightDf = _rightInput.getDataframe();
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    const auto& rows = *_rowOffsetState._offsetVec;
    const size_t rowOffsetIdx = _rowOffsetState._rowOffsetIdx;

    const auto& outCols = outDf->cols();
    const auto& cols = rightDf->cols();

    const size_t rowsToCopy = std::min(rows.size() - rowOffsetIdx, rowsRemaining);

    size_t columnOffset = _leftRowLen;
    for (size_t j = 0; j < rightDf->size(); ++j) {
        //Skip Columns present in the left input and the join key
        if (leftDf->getColumn(cols[j]->getTag()) != nullptr ||
            cols[j]->getTag() == _rightJoinKey) {
            continue;
        }

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

template <typename Key>
void HashJoinProcessor<Key>::flushLeftStream(size_t& rowsRemaining, size_t& totalRowsInserted) {
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

template class db::HashJoinProcessor<NodeID>;
template class db::HashJoinProcessor<int64_t>;
template class db::HashJoinProcessor<uint64_t>;
template class db::HashJoinProcessor<double>;
template class db::HashJoinProcessor<std::string_view>;
template class db::HashJoinProcessor<CustomBool>;
