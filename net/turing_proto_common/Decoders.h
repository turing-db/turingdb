#pragma once

#include <stack>
#include <stddef.h>
#include <cstddef>
#include <cstring>
#include <span>
#include <string>
#include <string_view>

#include "ChunkedBuffer.h"
#include "GraphPath.h"
#include "LocalMemory.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "list/ListBuffer.h"
#include "list/ListUtils.h"

namespace net::proto {

/**
 * @brief Reads one list element off the wire and appends it into the list buffer's
 * reserved space, dispatched on the element's tag via @ref db::ListTagDispatcher.
 *
 * The element's tag is glanced (not yet consumed) by the caller; this visitor consumes
 * the tag together with its value once enough is buffered. Returns true if the element
 * was fully consumed, false if the read buffer ran dry: either before the element could
 * start (nothing consumed, retried next time) or partway through a String/Embedding
 * payload (the view is already appended and its bytes resume via @ref _bufferState).
 */
struct ListElementReadVisitor {
    TuringProtoDecoder::DecodeContext* _ctx {nullptr};
    // Optional: if set, receives the view of each element appended to the list buffer
    // (used by the ListElementView columns, which store one view per row).
    db::ListElementView* _outView {nullptr};

    template <typename T>
    bool operator()(const db::ListElementView unusedView) const {
        constexpr size_t tagSize = sizeof(db::ListBufferTypeTag);
        auto& currCursor = _ctx->_listStack.top();

        if constexpr (std::is_same_v<T, db::types::String::Primitive>) {
            if (_ctx->_inBuf->readable() < tagSize + sizeof(WireSize)) {
                return false;
            }
            _ctx->_inBuf->increaseReadOffset(tagSize);

            WireSize numBytes = 0;
            _ctx->_inBuf->readData(&numBytes, sizeof(numBytes));

            char* dest = _ctx->_stringBuffer->alloc(numBytes);
            const T view = _ctx->_stringBuffer->getView(dest, numBytes);
            captureOutView(currCursor.writeValue<T>(db::TypeToListBufferTag<T>::Tag, view));

            return readVarLenPayload(dest, numBytes);
        } else if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
            if (_ctx->_inBuf->readable() < tagSize + sizeof(WireSize)) {
                return false;
            }
            _ctx->_inBuf->increaseReadOffset(tagSize);

            WireSize numBytes = 0;
            _ctx->_inBuf->readData(&numBytes, sizeof(numBytes));

            const size_t numFloats = numBytes / sizeof(float);
            float* dest = _ctx->_embeddingBuffer->alloc(numFloats);
            const T view = _ctx->_embeddingBuffer->getView(dest, numFloats);
            captureOutView(currCursor.writeValue<T>(db::TypeToListBufferTag<T>::Tag, view));

            return readVarLenPayload(reinterpret_cast<char*>(dest), numBytes);
        } else if constexpr (std::is_same_v<T, db::ListView>) {
            if (_ctx->_inBuf->readable() < tagSize + sizeof(WireSize) + sizeof(WireSize)) {
                return false;
            }
            _ctx->_inBuf->increaseReadOffset(tagSize);

            WireSize numElements = 0;
            WireSize numBytes = 0;
            _ctx->_inBuf->readData(&numElements, sizeof(numElements));
            _ctx->_inBuf->readData(&numBytes, sizeof(numBytes));

            // The parent's nested slot stores the child's ListView; the child's elements
            // then stream into the cursor we push.
            const db::ListWriteCursor childCursor = _ctx->_listBuffer->reserveList(numElements, numBytes);
            captureOutView(currCursor.writeValue<T>(db::TypeToListBufferTag<T>::Tag, childCursor.getView()));
            _ctx->_listStack.emplace(childCursor);

            return true;
        } else {
            // Fixed-width element: the wire layout [tag][value] is identical to the stored
            // layout, so copy it straight into the reserved slot — no intermediate value.
            const size_t elementBytes = tagSize + sizeof(T);
            if (_ctx->_inBuf->readable() < elementBytes) {
                return false;
            }

            captureOutView(currCursor.writeRaw(_ctx->_inBuf->readPtr(), elementBytes));
            _ctx->_inBuf->increaseReadOffset(elementBytes);

            return true;
        }
    }

private:
    // If a caller asked for it (the ListElementView columns, which store one view per row),
    // hands back the view of the element just written.
    void captureOutView(const db::ListElementView view) const {
        if (_outView) {
            *_outView = view;
        }
    }

    // Copies @param numBytes into the (stable) @param dest, queueing a @ref _bufferState
    // resume if the payload is split across the buffer boundary.
    bool readVarLenPayload(char* dest, WireSize numBytes) const {
        if (numBytes <= _ctx->_inBuf->readable()) {
            _ctx->_inBuf->readData(dest, numBytes);
            return true;
        }

        const size_t numBytesToRead = _ctx->_inBuf->readable();
        _ctx->_inBuf->readData(dest, numBytesToRead);

        _ctx->_bufferState._start = dest;
        _ctx->_bufferState._len = numBytes;
        _ctx->_bufferState._offset = numBytesToRead;
        return false;
    }
};

/**
 * @brief Drains the list cursor stack in @ref _listStack, reading elements off the wire into
 * the reserved buffers until the stack empties (the whole top-level list is decoded) or the
 * read buffer runs dry (returns false; the stack and @ref _bufferState carry the resume point).
 *
 * For each top-level element completed (one written through the column's own cursor, i.e. stack
 * depth 1 — nested elements live deeper and are not column rows), @param onTopLevelElement is
 * invoked with its 0-based index and view. ListElementView columns store the view; ListView
 * columns pass a no-op.
 *
 * return false if we have an incomplete element in the input buffer.
 */
template <typename OnTopLevelElement>
inline bool drainListStack(TuringProtoDecoder::DecodeContext* ctx,
                           const OnTopLevelElement& onTopLevelElement) {
    db::ListElementView view;
    const ListElementReadVisitor readVisitor {ctx, &view};
    auto& stack = ctx->_listStack;

    while (!stack.empty()) {
        if (stack.top().isComplete()) {
            stack.pop();
            continue;
        }

        if (ctx->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
            return false;
        }

        db::ListBufferTypeTag tag {};
        // Peek the tag (don't consume): keeping it in the buffer lets the fixed-width path
        // memcpy [tag][value] in one go.
        memcpy(&tag, ctx->_inBuf->readPtr(), sizeof(tag));

        // A top-level element is one written through the column's own cursor (stack depth 1);
        // nested elements live deeper and are not column rows. The reference stays valid across
        // a push (std::stack is deque-backed, which doesn't relocate existing elements).
        const bool topLevel = (stack.size() == 1);
        db::ListWriteCursor& cursor = stack.top();
        const size_t elementIndex = cursor.getWritten();

        const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});

        if (topLevel && cursor.getWritten() > elementIndex) {
            onTopLevelElement(elementIndex, view);
        }
        if (!elementComplete) {
            return false;
        }
    }

    return true;
}

template <typename T>
inline bool decodeVector(TuringProtoDecoder::DecodeContext* ctx,
                         db::ColumnVector<T>* typedCol,
                         DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeVector");

    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->getNumRows());
    }

    const size_t rowsRemaining = columnState->getNumRows() - ctx->_rowIndex;

    if (rowsRemaining * sizeof(T) > ctx->_inBuf->readable()) {
        const size_t numRowsRead = ctx->_inBuf->readable() / sizeof(T);
        ctx->_inBuf->readData(typedCol->data() + ctx->_rowIndex, numRowsRead * sizeof(T));
        ctx->_rowIndex += numRowsRead;
        return false;
    }

    ctx->_inBuf->readData(typedCol->data() + ctx->_rowIndex, rowsRemaining * sizeof(T));
    return true;
}

template <>
inline bool decodeVector<std::string_view>(TuringProtoDecoder::DecodeContext* ctx,
                                      db::ColumnVector<std::string_view>* typedCol,
                                      DfColumnState* columnState) {
    const size_t numRows = columnState->getNumRows();

    if (ctx->_rowIndex == 0) {
        typedCol->reserve(numRows);
    }

    const size_t startIndex = ctx->_rowIndex;
    TuringProtoInBuf* inBuf = ctx->_inBuf;
    const size_t readableRemaining = inBuf->readable();

    for (size_t i = 0; startIndex + i < numRows; i++) {
        if (readableRemaining < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        WireSize stringSize = 0;
        inBuf->readData(&stringSize, sizeof(stringSize));

        db::StringBuffer& strBuf = ctx->_mem->stringBuffer();

        // Entire string is in this buffer
        if (stringSize <= readableRemaining) {
            const char* startPtr = inBuf->readPtr();
            const std::string_view wireString {startPtr, stringSize};

            const std::string_view bufferString = strBuf.insert(wireString);
            typedCol->emplace_back(bufferString);
            continue;
        }

        // String starts but does not finish in this buffer

        // Temporary used to copy bytes into buffer
        std::string tmpBuffer(stringSize, '\0');
        inBuf->readData(tmpBuffer.data(), readableRemaining);

        // Write the chars which exists in this buffer, plus additional space for the
        // remainder of the string into the string buffer
        char* svStart = strBuf.nextPtr();
        std::string_view bufferString = strBuf.insert(tmpBuffer);
        // The column will hold a view onto the to-be-finished string
        typedCol->emplace_back(bufferString);

        // Update the buffer state so @ref decodeIncomingChunk completes this string
        ctx->_bufferState._start = svStart;
        ctx->_bufferState._len = stringSize;
        ctx->_bufferState._offset = readableRemaining;

        ctx->_rowIndex += i + 1;
        return false;
    }

    return true;
}

template <>
inline bool decodeVector<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                      db::ColumnVector<std::string>* typedCol,
                                      DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->getNumRows());
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        WireSize stringSize = 0;
        ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

        if (stringSize <= ctx->_inBuf->readable()) {
            typedCol->emplace_back(ctx->_inBuf->readPtr(), stringSize);
            ctx->_inBuf->increaseReadOffset(stringSize);
        } else {
            auto& val = typedCol->emplace_back(stringSize, '\0');
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(val.data(), numBytesToRead);

            ctx->_bufferState._start = val.data();
            ctx->_bufferState._len = val.size();
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::ListView>(TuringProtoDecoder::DecodeContext* ctx,
                                       db::ColumnVector<db::ListView>* typedCol,
                                       DfColumnState* columnState) {
    size_t numRows = columnState->getNumRows();
    if (ctx->_rowIndex == 0 && typedCol->size() == 0) {
        typedCol->reserve(numRows);
    }

    auto& stack = ctx->_listStack;

    while (ctx->_rowIndex < numRows) {
        // A list is "started" once we have read its header and emplaced its (initially
        // unfilled) ListView. col->size() vs _rowIndex tells us which: equal means the
        // current row's header is still to come.
        const bool listStarted = (typedCol->size() == ctx->_rowIndex + 1);
        if (!listStarted) {
            if (ctx->_inBuf->readable() < 2 * sizeof(WireSize)) {
                return false;
            }

            WireSize elementCount = 0;
            WireSize listByteSize = 0;
            ctx->_inBuf->readData(&elementCount, sizeof(elementCount));
            ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            stack.emplace(ctx->_listBuffer->reserveList(elementCount, listByteSize));
            typedCol->emplace_back(stack.top().getView());
        }

        // Stream this row's list (and any nested children) straight into the reserved space.
        auto onTopLevelElement = [](size_t, const db::ListElementView&) {};
        if (!drainListStack(ctx, onTopLevelElement)) {
            return false;
        }

        ++ctx->_rowIndex;
    }

    return true;
}

// A ListElementView column has one element per column row, wire-encoded as [listByteSize]
// followed by the elements. Each element is streamed onto the list buffer and its view
// stored in the corresponding row.
template <>
inline bool decodeVector<db::ListElementView>(TuringProtoDecoder::DecodeContext* ctx,
                                              db::ColumnVector<db::ListElementView>* typedCol,
                                              DfColumnState* columnState) {
    auto& stack = ctx->_listStack;

    // If the row index is 0 this indicates that we haven't read any values from
    // our encoded ListView and need to allocate the list buffer itself
    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            return false;
        }

        WireSize listByteSize = 0;
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        stack.emplace(ctx->_listBuffer->reserveList(columnState->getNumRows(), listByteSize));
        typedCol->resize(columnState->getNumRows());
        ctx->_rowIndex = 1;
    }

    // One column row per top-level element; its view is stored in the matching row.
    auto onTopLevelElement = [typedCol](size_t index, const db::ListElementView& view) {
        typedCol->data()[index] = view;
    };
    return drainListStack(ctx, onTopLevelElement);
}

template <>
inline bool decodeVector<db::Path>(TuringProtoDecoder::DecodeContext* ctx,
                                   db::ColumnVector<db::Path>* typedCol,
                                   DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->getNumRows());
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        WireSize pathByteSize = 0;
        ctx->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
        const size_t numEntities = pathByteSize / sizeof(db::EntityID);

        if (pathByteSize <= ctx->_inBuf->readable()) {
            auto& path = typedCol->emplace_back(numEntities);
            ctx->_inBuf->readData(path.data(), pathByteSize);
        } else {
            auto& path = typedCol->emplace_back(numEntities);
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(path.data(), numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(path.data());
            ctx->_bufferState._len = pathByteSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                          db::ColumnVector<db::types::Embedding::Primitive>* typedCol,
                                                          DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->getNumRows());
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        WireSize embeddingSize = 0;
        ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
        const size_t numFloats = embeddingSize / sizeof(float);

        auto* data = ctx->_embeddingBuffer->alloc(numFloats);
        const std::span<const float> span = ctx->_embeddingBuffer->getView(data, numFloats);

        if (embeddingSize <= ctx->_inBuf->readable()) {
            ctx->_inBuf->readData(data, embeddingSize);
            typedCol->emplace_back(span);
        } else {
            typedCol->emplace_back(span);

            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(data, numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(data);
            ctx->_bufferState._len = embeddingSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::EntityList>(TuringProtoDecoder::DecodeContext* ctx,
                                         db::ColumnVector<db::EntityList>* typedCol,
                                         DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->getNumRows());
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        auto& entityList = (*typedCol)[ctx->_rowIndex + i];

        if (entityList.size() == 0) {
            if (ctx->_inBuf->readable() < sizeof(WireSize)) {
                ctx->_rowIndex += i;
                return false;
            }

            WireSize numberOfEntries = 0;
            ctx->_inBuf->readData(&numberOfEntries, sizeof(numberOfEntries));
            entityList.reserve(numberOfEntries);
        }

        while ((entityList.size() < entityList.capacity())) {
            if (ctx->_inBuf->readable() < sizeof(db::EntityList::Entry::_type)) {
                ctx->_rowIndex += i;
                return false;
            }

            entityList.add();

            ctx->_inBuf->readData(&entityList.back()._type, sizeof(db::EntityList::Entry::_type));

            if (ctx->_inBuf->readable() < sizeof(db::EntityList::Entry::_id)) {
                throw TuringException("Entity List Entry can't be broken up across packet/buffer boundaries");
            }

            ctx->_inBuf->readData(&entityList.back()._id, sizeof(db::EntityList::Entry::_id));
        }
    }

    return true;
}

template <typename T>
inline bool decodeOptVector(TuringProtoDecoder::DecodeContext* ctx,
                            db::ColumnOptVector<T>* typedCol,
                            DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptVector");

    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(T)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->getBitMask().test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(T));
            continue;
        }

        T value {};
        ctx->_inBuf->readData(&value, sizeof(T));
        entry = value;
    }
    return true;
}

template <>
inline bool decodeOptVector<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                         db::ColumnOptVector<std::string>* typedCol,
                                         DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->getBitMask().test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(WireSize));
            continue;
        }

        WireSize stringSize = 0;
        ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

        if (stringSize <= ctx->_inBuf->readable()) {
            entry.emplace(ctx->_inBuf->readPtr(), stringSize);
            ctx->_inBuf->increaseReadOffset(stringSize);
        } else {
            auto& val = entry.emplace(stringSize, '\0');
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(val.data(), numBytesToRead);

            ctx->_bufferState._start = val.data();
            ctx->_bufferState._len = val.size();
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeOptVector<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                              db::ColumnOptVector<db::types::Embedding::Primitive>* typedCol,
                                                              DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->getNumRows(); ++i) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->getBitMask().test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(WireSize));
            continue;
        }

        WireSize embeddingSize = 0;
        ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
        const size_t numFloats = embeddingSize / sizeof(float);

        auto* data = ctx->_embeddingBuffer->alloc(numFloats);
        const std::span<const float> span = ctx->_embeddingBuffer->getView(data, numFloats);

        if (embeddingSize <= ctx->_inBuf->readable()) {
            ctx->_inBuf->readData(data, embeddingSize);
            entry.emplace(span);
        } else {
            entry.emplace(span);

            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(data, numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(data);
            ctx->_bufferState._len = embeddingSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <typename T>
inline bool decodeConst(TuringProtoDecoder::DecodeContext* ctx,
                        db::ColumnConst<T>* typedCol) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeConst");

    if (ctx->_inBuf->readable() < sizeof(T)) {
        return false;
    }

    T value {};
    ctx->_inBuf->readData(&value, sizeof(T));
    typedCol->set(value);
    return true;
}

template <>
inline bool decodeConst<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                     db::ColumnConst<std::string>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize stringSize = 0;
    ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

    if (stringSize <= ctx->_inBuf->readable()) {
        std::string val(ctx->_inBuf->readPtr(), stringSize);
        typedCol->set(std::move(val));
        ctx->_inBuf->increaseReadOffset(stringSize);
    } else {
        std::string val(stringSize, '\0');
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(val.data(), numBytesToRead);

        ctx->_bufferState._start = val.data();
        ctx->_bufferState._len = val.size();
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(val));
        // Const columns have no row dimension, so queueing a body-resume via
        // _bufferState means the whole column is decoded once that resume
        // finishes. Mark this column complete now (advance _colIndex; reset
        // _rowIndex for the next column) so when more bytes arrive,
        // decodeIncomingChunk drains the body via _bufferState and then
        // decodeIncomingData proceeds to the *next* column instead of
        // re-entering this decoder and reading the next column's bytes as
        // fresh framing. Mirrors how decodeOptVector advances _rowIndex
        // before its own buffer-state queue.
        ++ctx->_colIndex;
        ctx->_rowIndex = 0;
        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::Path>(TuringProtoDecoder::DecodeContext* ctx,
                                  db::ColumnConst<db::Path>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize pathByteSize = 0;
    ctx->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
    const size_t numEntities = pathByteSize / sizeof(db::EntityID);

    if (pathByteSize <= ctx->_inBuf->readable()) {
        db::Path path(numEntities);
        ctx->_inBuf->readData(path.data(), pathByteSize);
        typedCol->set(std::move(path));
    } else {
        db::Path path(numEntities);
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(path.data(), numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(path.data());
        ctx->_bufferState._len = pathByteSize;
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(path));
        // See decodeConst<std::string>: const columns have no row dimension,
        // so completing this body via _bufferState completes the column.
        // Advance _colIndex to keep decodeIncomingData from re-entering and
        // misreading the next column's bytes as path framing.
        ++ctx->_colIndex;
        ctx->_rowIndex = 0;
        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                         db::ColumnConst<db::types::Embedding::Primitive>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize embeddingSize = 0;
    ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
    const size_t numFloats = embeddingSize / sizeof(float);

    auto* data = ctx->_embeddingBuffer->alloc(numFloats);
    const std::span<const float> span =
        ctx->_embeddingBuffer->getView(data, numFloats);

    if (embeddingSize <= ctx->_inBuf->readable()) {
        ctx->_inBuf->readData(data, embeddingSize);
        typedCol->set(span);
    } else {
        typedCol->set(span);

        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(data, numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(data);
        ctx->_bufferState._len = embeddingSize;
        ctx->_bufferState._offset = numBytesToRead;

        // See decodeConst<std::string>: const columns have no row dimension,
        // so completing this body via _bufferState completes the column.
        // Advance _colIndex to keep decodeIncomingData from re-entering and
        // misreading the next column's bytes as embedding framing.
        ++ctx->_colIndex;
        ctx->_rowIndex = 0;
        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::ListView>(TuringProtoDecoder::DecodeContext* ctx,
                                      db::ColumnConst<db::ListView>* typedCol) {
    auto& stack = ctx->_listStack;

    // Const columns have no row dimension, so reuse _rowIndex as a 0/1 "list started"
    // marker (the driver resets it to 0 before this column). 1 means we have read the
    // header, reserved the space, and set the (initially unfilled) ListView on the column.
    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < 2 * sizeof(WireSize)) {
            return false;
        }

        WireSize elementCount = 0;
        WireSize listByteSize = 0;
        ctx->_inBuf->readData(&elementCount, sizeof(elementCount));
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        stack.emplace(ctx->_listBuffer->reserveList(elementCount, listByteSize));
        typedCol->set(stack.top().getView());
        ctx->_rowIndex = 1;
    }
    auto onTopLevelElement = [](size_t, const db::ListElementView&) {};
    return drainListStack(ctx, onTopLevelElement);
}

// A constant ListElementView is a single element, wire-encoded as [listByteSize] + one
// element. Stream it onto the list buffer and set the column to its view.
template <>
inline bool decodeConst<db::ListElementView>(TuringProtoDecoder::DecodeContext* ctx,
                                             db::ColumnConst<db::ListElementView>* typedCol) {
    auto& stack = ctx->_listStack;

    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < sizeof(WireSize)) {
            return false;
        }

        WireSize listByteSize = 0;
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        stack.emplace(ctx->_listBuffer->reserveList(1, listByteSize));
        ctx->_rowIndex = 1;
    }

    // The constant is the single top-level element; store its view on the column.
    auto onTopLevelElement = [typedCol](size_t, const db::ListElementView& view) {
        typedCol->set(view);
    };

    return drainListStack(ctx, onTopLevelElement); 
}

template <>
inline bool decodeConst<db::PropertyNull>(TuringProtoDecoder::DecodeContext* ctx,
                                          db::ColumnConst<db::PropertyNull>* typedCol) {
    return true;
}

template <typename T>
inline bool decodeOptConst(TuringProtoDecoder::DecodeContext* ctx,
                           db::ColumnConst<std::optional<T>>* typedCol) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptConst");

    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(T)) {
        return false;
    }

    T value {};
    ctx->_inBuf->readData(&value, sizeof(T));
    typedCol->set(value);
    return true;
}

template <>
inline bool decodeOptConst<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                        db::ColumnConst<std::optional<std::string>>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize stringSize = 0;
    ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

    if (stringSize <= ctx->_inBuf->readable()) {
        typedCol->set(std::string(ctx->_inBuf->readPtr(), stringSize));
        ctx->_inBuf->increaseReadOffset(stringSize);
    } else {
        std::string val(stringSize, '\0');
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(val.data(), numBytesToRead);

        ctx->_bufferState._start = val.data();
        ctx->_bufferState._len = val.size();
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(val));
        // See decodeConst<std::string>: const columns have no row dimension,
        // so completing this body via _bufferState completes the column.
        // Advance _colIndex to keep decodeIncomingData from re-entering and
        // misreading the next column's hasValue/length bytes as this column's
        // framing.
        ++ctx->_colIndex;
        ctx->_rowIndex = 0;
        return false;
    }
    return true;
}

template <>
inline bool decodeOptConst<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                            db::ColumnConst<std::optional<db::types::Embedding::Primitive>>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize embeddingSize = 0;
    ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
    const size_t numFloats = embeddingSize / sizeof(float);

    auto* data = ctx->_embeddingBuffer->alloc(numFloats);
    const std::span<const float> span = ctx->_embeddingBuffer->getView(data, numFloats);

    if (embeddingSize <= ctx->_inBuf->readable()) {
        ctx->_inBuf->readData(data, embeddingSize);
        typedCol->set(span);
    } else {
        typedCol->set(span);

        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(data, numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(data);
        ctx->_bufferState._len = embeddingSize;
        ctx->_bufferState._offset = numBytesToRead;

        // See decodeConst<std::string>: const columns have no row dimension,
        // so completing this body via _bufferState completes the column.
        // Advance _colIndex to keep decodeIncomingData from re-entering and
        // misreading the next column's bytes as embedding framing.
        ++ctx->_colIndex;
        ctx->_rowIndex = 0;
        return false;
    }
    return true;
}

}
