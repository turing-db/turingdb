#pragma once

#include <stddef.h>
#include <span>
#include <string>

#include "ChunkedBuffer.h"
#include "GraphPath.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoDecoderConcepts.h"
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

        if constexpr (std::is_same_v<T, db::types::String::Primitive>) {
            if (_ctx->_inBuf->readable() < tagSize + sizeof(uint32_t)) {
                return false;
            }
            _ctx->_inBuf->increaseReadOffset(tagSize);

            uint32_t numBytes = 0;
            _ctx->_inBuf->readData(&numBytes, sizeof(numBytes));

            char* dest = _ctx->_stringBuffer->alloc(numBytes);
            appended(_ctx->_listBuffer->appendElement(_ctx->_stringBuffer->getView(dest, numBytes)));

            return readVarLenPayload(dest, numBytes);
        } else if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
            if (_ctx->_inBuf->readable() < tagSize + sizeof(uint32_t)) {
                return false;
            }
            _ctx->_inBuf->increaseReadOffset(tagSize);

            uint32_t numBytes = 0;
            _ctx->_inBuf->readData(&numBytes, sizeof(numBytes));

            const size_t numFloats = numBytes / sizeof(float);
            float* dest = _ctx->_embeddingBuffer->alloc(numFloats);
            appended(_ctx->_listBuffer->appendElement(_ctx->_embeddingBuffer->getView(dest, numFloats)));

            return readVarLenPayload(reinterpret_cast<char*>(dest), numBytes);
        } else if constexpr (std::is_same_v<T, db::ListView>) {
            throw TuringException("Nested lists are not supported over the binary protocol");
        } else {
            // Fixed-width element: the wire layout [tag][value] is identical to the stored
            // layout, so copy it straight into the reserved slot — no intermediate value.
            const size_t elementBytes = tagSize + sizeof(T);
            if (_ctx->_inBuf->readable() < elementBytes) {
                return false;
            }

            appended(_ctx->_listBuffer->appendRawElement(_ctx->_inBuf->readPtr(), elementBytes));
            _ctx->_inBuf->increaseReadOffset(elementBytes);

            return true;
        }
    }

private:
    // Records that an element was appended: advances the cursor and, if a caller asked
    // for it, hands back the element's view.
    void appended(const db::ListElementView view) const {
        ++_ctx->_listWritten;
        if (_outView != nullptr) {
            *_outView = view;
        }
    }

    // Copies @param numBytes into the (stable) @param dest, queueing a @ref _bufferState
    // resume if the payload is split across the buffer boundary.
    bool readVarLenPayload(char* dest, uint32_t numBytes) const {
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

template <typename T>
inline bool decodeVector(TuringProtoDecoder::DecodeContext* ctx,
                         db::ColumnVector<T>* typedCol,
                         TuringProtoDecoder::DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeVector");

    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->_numRows);
    }

    const size_t rowsRemaining = columnState->_numRows - ctx->_rowIndex;

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
inline bool decodeVector<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                      db::ColumnVector<std::string>* typedCol,
                                      TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t stringSize = 0;
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
                                       TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0 && typedCol->size() == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    const ListElementReadVisitor readVisitor {ctx};

    while (ctx->_rowIndex < columnState->_numRows) {
        // A list is "started" once we have read its header and emplaced its (initially
        // unfilled) ListView. col->size() vs _rowIndex tells us which: equal means the
        // current row's header is still to come.
        const bool listStarted = (typedCol->size() == ctx->_rowIndex + 1);
        if (!listStarted) {
            if (ctx->_inBuf->readable() < 2 * sizeof(uint32_t)) {
                return false;
            }

            uint32_t elementCount = 0;
            uint32_t listByteSize = 0;
            ctx->_inBuf->readData(&elementCount, sizeof(elementCount));
            ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            typedCol->emplace_back(ctx->_listBuffer->reserveList(elementCount, listByteSize));
            ctx->_listCount = elementCount;
            ctx->_listWritten = 0;
        }

        // Stream the remaining elements straight into the reserved space.
        while (ctx->_listWritten < ctx->_listCount) {
            if (ctx->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
                return false;
            }

            db::ListBufferTypeTag tag {};
            // This 'peek' of the ListBufferTypeTag preserves the tag bytes in the buffer that
            // later lets us memcpy the inline tag + fixed-size data in one go
            memcpy(&tag, ctx->_inBuf->readPtr(), sizeof(tag));

            const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});
            if (!elementComplete) {
                return false;
            }
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
                                              TuringProtoDecoder::DfColumnState* columnState) {
    // We pass this view object to the read visitor as a way of returning the ListElementView of
    // the object we just inserted.
    db::ListElementView view;
    const ListElementReadVisitor readVisitor {ctx, &view};

    // If the row index is 0 this indicates that we haven't read any values from
    // our encoded ListView and need to allocate the list buffer itself
    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            return false;
        }

        uint32_t listByteSize = 0;
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        ctx->_listBuffer->reserveList(columnState->_numRows, listByteSize);
        typedCol->resize(columnState->_numRows);
        ctx->_listCount = columnState->_numRows;
        ctx->_listWritten = 0;
        ctx->_rowIndex = 1;
    }

    while (ctx->_listWritten < ctx->_listCount) {
        if (ctx->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
            return false;
        }

        db::ListBufferTypeTag tag {};
        memcpy(&tag, ctx->_inBuf->readPtr(), sizeof(tag));

        const size_t elementIndex = ctx->_listWritten;
        const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});

        // Append the view of the list element we just inserted into the list buffer
        if (ctx->_listWritten > elementIndex) {
            typedCol->data()[elementIndex] = view;
        }
        if (!elementComplete) {
            return false;
        }
    }

    return true;
}

template <>
inline bool decodeVector<db::Path>(TuringProtoDecoder::DecodeContext* ctx,
                                   db::ColumnVector<db::Path>* typedCol,
                                   TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t pathByteSize = 0;
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
                                                          TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t embeddingSize = 0;
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
                                         TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        auto& entityList = (*typedCol)[ctx->_rowIndex + i];

        if (entityList.size() == 0) {
            if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
                ctx->_rowIndex += i;
                return false;
            }

            uint32_t numberOfEntries = 0;
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
                            TuringProtoDecoder::DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptVector");

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(T)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
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
                                         TuringProtoDecoder::DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(uint32_t));
            continue;
        }

        uint32_t stringSize = 0;
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
                                                              TuringProtoDecoder::DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(uint32_t));
            continue;
        }

        uint32_t embeddingSize = 0;
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
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t stringSize = 0;
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
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t pathByteSize = 0;
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
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t embeddingSize = 0;
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
    const ListElementReadVisitor readVisitor {ctx};

    // Const columns have no row dimension, so reuse _rowIndex as a 0/1 "list started"
    // marker (the driver resets it to 0 before this column). 1 means we have read the
    // header, reserved the space, and set the (initially unfilled) ListView on the column.
    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < 2 * sizeof(uint32_t)) {
            return false;
        }

        uint32_t elementCount = 0;
        uint32_t listByteSize = 0;
        ctx->_inBuf->readData(&elementCount, sizeof(elementCount));
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        typedCol->set(ctx->_listBuffer->reserveList(elementCount, listByteSize));
        ctx->_listCount = elementCount;
        ctx->_listWritten = 0;
        ctx->_rowIndex = 1;
    }

    while (ctx->_listWritten < ctx->_listCount) {
        if (ctx->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
            return false;
        }

        db::ListBufferTypeTag tag {};
        memcpy(&tag, ctx->_inBuf->readPtr(), sizeof(tag));

        const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});
        if (!elementComplete) {
            return false;
        }
    }

    return true;
}

// A constant ListElementView is a single element, wire-encoded as [listByteSize] + one
// element. Stream it onto the list buffer and set the column to its view.
template <>
inline bool decodeConst<db::ListElementView>(TuringProtoDecoder::DecodeContext* ctx,
                                             db::ColumnConst<db::ListElementView>* typedCol) {
    db::ListElementView view;
    const ListElementReadVisitor readVisitor {ctx, &view};

    if (ctx->_rowIndex == 0) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            return false;
        }

        uint32_t listByteSize = 0;
        ctx->_inBuf->readData(&listByteSize, sizeof(listByteSize));

        ctx->_listBuffer->reserveList(1, listByteSize);
        ctx->_listCount = 1;
        ctx->_listWritten = 0;
        ctx->_rowIndex = 1;
    }

    while (ctx->_listWritten < ctx->_listCount) {
        if (ctx->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
            return false;
        }

        db::ListBufferTypeTag tag {};
        memcpy(&tag, ctx->_inBuf->readPtr(), sizeof(tag));

        const size_t elementIndex = ctx->_listWritten;
        const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});

        if (ctx->_listWritten > elementIndex) {
            typedCol->set(view);
        }
        if (!elementComplete) {
            return false;
        }
    }

    return true;
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

    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t stringSize = 0;
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

    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t embeddingSize = 0;
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
