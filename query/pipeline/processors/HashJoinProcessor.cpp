#include "HashJoinProcessor.h"

#include "ExecutionContext.h"
#include "RowStore.h"
#include "columns/ColumnDispatcher.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "BioAssert.h"

using namespace db;

namespace {

template <typename Key>
using UnwrappedKey = TypeUtils::unwrap_optional_t<Key>;

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
using LookupKey = std::conditional_t<StringLike<UnwrappedKey<Key>>,
                                     std::string_view,
                                     UnwrappedKey<Key>>;

// Extracts the join key from the column as a lightweight value suitable for
// map lookups. For string-like keys returns std::string_view (no allocation).
// If the join key is null we return a nullopt to the user to allow them to skip this key
template <typename Key>
[[nodiscard]] std::optional<LookupKey<Key>> tryExtractKeyView(Column* col, size_t index) {
    const auto* typedCol = static_cast<const ColumnVector<Key>*>(col);

    if constexpr (TypeUtils::is_optional_v<Key>) {
        const auto& optional = (*typedCol)[index];
        if (!optional.has_value()) {
            return std::nullopt;
        }

        if constexpr (StringLike<UnwrappedKey<Key>>) {
            return std::string_view(*optional);
        } else {
            return *optional;
        }
    } else {
        const auto& value = (*typedCol)[index];

        if constexpr (StringLike<UnwrappedKey<Key>>) {
            return std::string_view(value);
        } else {
            return value;
        }
    }
}

// Looks up or inserts the key in the map, returning the offset vector.
// For string keys, uses transparent find to avoid allocation when the key exists.
std::vector<RowOffset>& getMap(auto& map, const auto& key) {
    using KeyType = std::decay_t<decltype(key)>;
    if constexpr (std::is_same_v<KeyType, std::string_view>) {
        const auto it = map.find(key);
        if (it != map.end()) {
            return it->second;
        }
        auto constructedEntry = map.try_emplace(std::string(key)).first;
        return constructedEntry->second;
    } else {
        return map[key];
    }
}

// Copies a single value from inputColumn[rowIndex] into outputColumn at
// [offset, offset+rowsToCopy), handling the case where the input and output
// column value types differ (e.g. string vs string_view).
void fillJoinColumn(Column* outputColumn,
                    Column* inputColumn,
                    size_t rowIndex,
                    size_t offset,
                    size_t rowsToCopy) {
    dispatchColumnVector(outputColumn, [&](auto* outCol) {
        using OutVal = typename std::decay_t<decltype(*outCol)>::ValueType;

        dispatchColumnVector(inputColumn, [&](auto* inCol) {
            using InVal = typename std::decay_t<decltype(*inCol)>::ValueType;

            if constexpr (std::is_constructible_v<OutVal, const InVal&>) {
                bioassert(outCol->size() >= offset + rowsToCopy,
                          "rows to copy do not fit in output column");
                std::fill_n(outCol->begin() + offset, rowsToCopy,
                            OutVal((*inCol)[rowIndex]));
            } else {
                throw PipelineException("Cannot fill input and output column of differing types");
            }
        });
    });
}
}

template <typename LeftKey, typename RightKey>
std::string HashJoinProcessor<LeftKey, RightKey>::describe() const {
    return fmt::format("HashJoinProcessor @={}", fmt::ptr(this));
}

template <typename LeftKey, typename RightKey>
HashJoinProcessor<LeftKey, RightKey>::HashJoinProcessor(const ColumnTag leftJoinKey,
                                                        const ColumnTag rightJoinKey)
    : _leftJoinKey(leftJoinKey),
    _rightJoinKey(rightJoinKey)
{
}

template <typename LeftKey, typename RightKey>
HashJoinProcessor<LeftKey,RightKey>* HashJoinProcessor<LeftKey, RightKey>::create(PipelineV2* pipeline,
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

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;

    const auto* leftDf = _leftInput.getDataframe();
    const auto* rightDf = _rightInput.getDataframe();
    const auto* outDf = _output.getDataframe();

    // The stored rows do not contain the join column
    _leftRowLen = leftDf->size() - 1;

    //We have already calculated what columns of the right
    //input are to be skipped when constructing the output dataframe
    //so we can get the length of the right output row by subtracting
    //the reserved space of the left columns and the join column from
    //the size of the output dataframe
    _rightRowLen = outDf->size() - _leftRowLen - 1;

    // Compute actual byte sizes of rows stored in the RowStore
    _leftRowByteSize = 0;
    for (const auto* namedCol : leftDf->cols()) {
        // RowStore doesn't store the join value so skip
        if (namedCol->getTag() == _leftJoinKey) {
            continue;
        }
        dispatchColumnVector(namedCol->getColumn(), [&](auto* col) {
            using ColType = std::remove_pointer_t<decltype(col)>;
            using ValType = typename ColType::ValueType;
            if constexpr (std::is_trivially_copyable_v<ValType>) {
                _leftRowByteSize += sizeof(ValType);
            } else {
                throw PipelineException("Left input column is not trivially copyable");
            }
        });
    }

    _rightRowByteSize = 0;
    for (const auto* namedCol : rightDf->cols()) {
        // skip the join value and any columns that are already stored in the left
        // RowStore
        if (leftDf->getColumn(namedCol->getTag()) != nullptr ||
            namedCol->getTag() == _rightJoinKey) {
            continue;
        }
        dispatchColumnVector(namedCol->getColumn(), [&](auto* col) {
            using ColType = std::remove_pointer_t<decltype(col)>;
            using ValType = typename ColType::ValueType;
            if constexpr (std::is_trivially_copyable_v<ValType>) {
                _rightRowByteSize += sizeof(ValType);
            } else {
                throw PipelineException("Right input column is not trivially copyable");
            }
        });
    }


    markAsPrepared();
}

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::reset() {
    _hasWritten = false;
    markAsReset();
}

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::execute() {
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
                                     col->resize(newOutputSize);
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
        // Trim output columns to the actual number of rows inserted
        for (const auto* namedCol : outDf->cols()) {
            dispatchColumnVector(namedCol->getColumn(),
                                 [&](auto* col) {
                                     col->resize(totalRowsInserted);
                                 });
        }
        _output.getPort()->writeData();
        _hasWritten = true;
    }

    // This covers the case when we have empty inputs to a processor. Writing
    // Data to the output port lets the pipeline cycle continue.
    if (_leftInput.getPort()->isClosed() && _rightInput.getPort()->isClosed()) {
        if (!_hasWritten) {
            // Trim output columns to 0 since no rows were produced
            for (const auto* namedCol : outDf->cols()) {
                dispatchColumnVector(namedCol->getColumn(),
                                     [&](auto* col) {
                                         col->resize(0);
                                     });
            }
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

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::processLeftStream(size_t& rowsRemaining,
                                                             size_t& totalRowsInserted) {
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();
    const size_t leftRowCount = leftDf->getLogicalRowCount();

    auto* leftCol = leftDf->getColumn(_leftJoinKey)->getColumn();

    // Resize output columns to chunkSize upfront; we trim after the loop.
    {
        const size_t chunkSize = _ctxt->getChunkSize();
        for (const auto* namedCol : outDf->cols()) {
            dispatchColumnVector(namedCol->getColumn(),
                                 [&](auto* col) {
                                     if (col->size() < chunkSize) {
                                         col->resize(chunkSize);
                                     }
                                 });
        }
    }

    for (; _leftInputIdx < leftRowCount; ++_leftInputIdx) {
        const auto leftKey = tryExtractKeyView<LeftKey>(leftCol, _leftInputIdx);
        // Skip null keys - NULL != NULL semantics
        if (!leftKey) {
            continue;
        }

        const auto it = _rightMap.find(*leftKey);
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

            size_t outColIdx = 0;
            for (size_t j = 0; j < leftDf->size(); ++j) {
                if (cols[j]->getTag() == _leftJoinKey) {
                    continue;
                }

                auto* inputColumn = cols[j]->getColumn();
                auto* outputColumn = outCols[outColIdx]->getColumn();

                dispatchColumnVector(outputColumn, [&](auto* outCol) {
                    const auto* typedInCol = static_cast<decltype(outCol)>(inputColumn);
                    fillOutputColumn(outCol,
                                     typedInCol,
                                     _leftInputIdx,
                                     totalRowsInserted,
                                     rowsToCopy);
                });
                ++outColIdx;
            }

            // Copy the join column to the last output column
            auto* inputColumn = leftDf->getColumn(_leftJoinKey)->getColumn();
            auto* outputColumn = outCols.back()->getColumn();

            dispatchColumnVector(outputColumn, [&](auto* col) {
                const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                fillOutputColumn(col,
                                 typedInCol,
                                 _leftInputIdx,
                                 totalRowsInserted,
                                 rowsToCopy);
            });

            // Copy over the stored rows starting at column _leftRowLen,
            // as we want to copy the left input into the first columns of the output.
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
        auto& offsetVec = getMap(_leftMap, *leftKey);
        offsetVec.emplace_back(_store.insertRow(leftDf,
                                                _leftJoinKey,
                                                _leftRowByteSize,
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
    const bool finishedProcessingInput = _leftInputIdx == leftRowCount;
    const bool needToCopyRowOffsets = _rowOffsetState.hasRowOffsets();
    if (!needToCopyRowOffsets && !finishedProcessingInput) {
        _leftInputIdx += 1;
    }
}

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::processRightStream(size_t& rowsRemaining,
                                                              size_t& totalRowsInserted) {
    const Dataframe* rightDf = _rightInput.getDataframe();
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();
    const size_t rightRowCount = rightDf->getLogicalRowCount();

    auto* rightCol = rightDf->getColumn(_rightJoinKey)->getColumn();

    // Resize output columns to chunkSize upfront; we trim after the loop.
    {
        const size_t chunkSize = _ctxt->getChunkSize();
        for (const auto* namedCol : outDf->cols()) {
            dispatchColumnVector(namedCol->getColumn(),
                                 [&](auto* col) {
                                     if (col->size() < chunkSize) {
                                         col->resize(chunkSize);
                                     }
                                 });
        }
    }

    for (; _rightInputIdx < rightRowCount; ++_rightInputIdx) {
        // Skip null keys - NULL != NULL semantics
        const auto rightKey = tryExtractKeyView<RightKey>(rightCol, _rightInputIdx);
        if (!rightKey) {
            continue;
        }

        const auto it = _leftMap.find(*rightKey);
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
                    const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
                    fillOutputColumn(col,
                                     typedInCol,
                                     _rightInputIdx,
                                     totalRowsInserted,
                                     rowsToCopy);
                });
                ++columnOffset;
            }

            // Copy the join column to the last output column
            fillJoinColumn(outCols.back()->getColumn(),
                           rightDf->getColumn(_rightJoinKey)->getColumn(),
                           _rightInputIdx,
                           totalRowsInserted,
                           rowsToCopy);

            for (size_t k = 0; k < rowsToCopy; ++k) {
                _store.copyRow(outDf,
                               0, // The left rows should be inserted from col index 0
                               totalRowsInserted + k,
                               _leftRowLen,
                               rows[k]);
            }
            totalRowsInserted += rowsToCopy;
        }

        auto& offsetVec = getMap(_rightMap, *rightKey);

        offsetVec.emplace_back(_store.insertRow(rightDf,
                                                leftDf,
                                                _rightJoinKey,
                                                _rightRowByteSize,
                                                _rightInputIdx));

        // break if we don't have any rows remaining to write
        if (rowsRemaining == 0) {
            break;
        }
    }

    // if we don't need to finish of copying a row offset vector
    // we can start from the next index
    const bool finishedProcessingInput = _rightInputIdx == rightRowCount;
    const bool needToCopyRowOffsets = _rowOffsetState.hasRowOffsets();
    if (!needToCopyRowOffsets && !finishedProcessingInput) {
        _rightInputIdx += 1;
    }
}

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::flushRightStream(size_t& rowsRemaining,
                                                            size_t& totalRowsInserted) {
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
            const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
            fillOutputColumn(col,
                             typedInCol,
                             _rightInputIdx,
                             totalRowsInserted,
                             rowsToCopy);
        });
        ++columnOffset;
    }

    // Copy the join column to the last output column
    fillJoinColumn(outCols.back()->getColumn(),
                   rightDf->getColumn(_rightJoinKey)->getColumn(),
                   _rightInputIdx,
                   totalRowsInserted,
                   rowsToCopy);

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

template <typename LeftKey, typename RightKey>
void HashJoinProcessor<LeftKey, RightKey>::flushLeftStream(size_t& rowsRemaining,
                                                           size_t& totalRowsInserted) {
    const Dataframe* leftDf = _leftInput.getDataframe();
    Dataframe* outDf = _output.getDataframe();

    const auto& rows = *_rowOffsetState._offsetVec;
    const size_t rowOffsetIdx = _rowOffsetState._rowOffsetIdx;

    const auto& cols = leftDf->cols();
    const auto& outCols = outDf->cols();

    const size_t rowsToCopy = std::min(rows.size() - rowOffsetIdx, rowsRemaining);

    size_t outColIdx = 0;
    for (size_t j = 0; j < leftDf->size(); ++j) {
        if (cols[j]->getTag() == _leftJoinKey) {
            continue;
        }

        auto* inputColumn = cols[j]->getColumn();
        auto* outputColumn = outCols[outColIdx]->getColumn();
        dispatchColumnVector(outputColumn, [&](auto* col) {
            const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
            fillOutputColumn(col,
                             typedInCol,
                             _leftInputIdx,
                             totalRowsInserted,
                             rowsToCopy);
        });
        ++outColIdx;
    }
    // Copy the join column to the last output column
    auto* inputColumn = leftDf->getColumn(_leftJoinKey)->getColumn();
    auto* outputColumn = outCols.back()->getColumn();

    dispatchColumnVector(outputColumn, [&](auto* col) {
        const auto* typedInCol = static_cast<decltype(col)>(inputColumn);
        fillOutputColumn(col,
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

#define HASH_JOIN_KEY_TYPES(X) \
    X(NodeID)                  \
    X(EdgeID)                  \
    X(int64_t)                 \
    X(uint64_t)                \
    X(double)                  \
    X(std::string_view)        \
    X(std::string)             \
    X(CustomBool)

#define INSTANTIATE_SAME(T)                  \
    template class db::HashJoinProcessor<T>; \
    template class db::HashJoinProcessor<std::optional<T>>;

#define INSTANTIATE_MIXED(T)                                   \
    template class db::HashJoinProcessor<T, std::optional<T>>; \
    template class db::HashJoinProcessor<std::optional<T>, T>;

HASH_JOIN_KEY_TYPES(INSTANTIATE_SAME)
HASH_JOIN_KEY_TYPES(INSTANTIATE_MIXED)

template class db::HashJoinProcessor<std::string_view, std::string>;
template class db::HashJoinProcessor<std::string, std::string_view>;
template class db::HashJoinProcessor<std::optional<std::string_view>, std::string>;
template class db::HashJoinProcessor<std::string_view, std::optional<std::string>>;
template class db::HashJoinProcessor<std::optional<std::string_view>, std::optional<std::string>>;
template class db::HashJoinProcessor<std::optional<std::string>, std::string_view>;
template class db::HashJoinProcessor<std::string, std::optional<std::string_view>>;
template class db::HashJoinProcessor<std::optional<std::string>, std::optional<std::string_view>>;
