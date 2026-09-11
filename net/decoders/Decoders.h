#pragma once

#include <stddef.h>
#include <cstring>
#include <optional>
#include <span>

#include "DecodedColumnSchema.h"
#include "GraphPath.h"
#include "DecodeContext.h"
#include "ProtoDecodeSink.h"
#include "TuringException.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "list/ListUtils.h"

namespace net::proto {

template <typename T>
inline size_t checkedElementCount(WireSize numBytes, const char* valueType) {
    if (numBytes % sizeof(T) != 0) {
        throw TuringException(std::string("Invalid ") + valueType + " byte size");
    }
    return numBytes / sizeof(T);
}

// Copies numBytes into the (stable) dest, queueing a resume through the
// context's buffer state if the payload is split across the buffer boundary.
inline bool readVarLenPayload(DecodeContext* context, char* dest, WireSize numBytes) {
    if (numBytes <= context->_inBuf->readable()) {
        context->_inBuf->readData(dest, numBytes);
        return true;
    }

    const size_t numBytesToRead = context->_inBuf->readable();
    context->_inBuf->readData(dest, numBytesToRead);

    context->_bufferState._start = dest;
    context->_bufferState._len = numBytes;
    context->_bufferState._offset = numBytesToRead;
    return false;
}

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
template <ProtoDecodeSink Sink>
struct ListElementReadVisitor {
    DecodeContext* _context {nullptr};
    Sink* _sink {nullptr};
    // Optional: if set, receives the view of each element appended to the list buffer
    // (used by the ListElementView columns, which store one view per row).
    SinkListElementView<Sink>* _outView {nullptr};

    template <typename T>
    bool operator()(const db::ListElementView unusedView) const {
        constexpr size_t tagSize = sizeof(db::ListBufferTypeTag);

        if constexpr (std::is_same_v<T, db::types::String::Primitive>) {
            if (_context->_inBuf->readable() < tagSize + sizeof(WireSize)) {
                return false;
            }
            _context->_inBuf->increaseReadOffset(tagSize);

            WireSize numBytes = 0;
            _context->_inBuf->readData(&numBytes, sizeof(numBytes));

            char* dest = _sink->allocString(numBytes);
            const T view = _sink->getStringView(dest, numBytes);
            captureOutView(_sink->writeListValue(view));

            return readVarLenPayload(_context, dest, numBytes);
        } else if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
            if (_context->_inBuf->readable() < tagSize + sizeof(WireSize)) {
                return false;
            }
            _context->_inBuf->increaseReadOffset(tagSize);

            WireSize numBytes = 0;
            _context->_inBuf->readData(&numBytes, sizeof(numBytes));

            const size_t numFloats = checkedElementCount<float>(numBytes, "embedding");
            float* dest = _sink->allocEmbedding(numFloats);
            const T view = _sink->getEmbeddingView(dest, numFloats);
            captureOutView(_sink->writeListValue(view));

            return readVarLenPayload(_context, reinterpret_cast<char*>(dest), numBytes);
        } else if constexpr (std::is_same_v<T, db::ListView>) {
            if (_context->_inBuf->readable() < tagSize + sizeof(WireSize) + sizeof(WireSize)) {
                return false;
            }
            _context->_inBuf->increaseReadOffset(tagSize);

            WireSize numElements = 0;
            WireSize numBytes = 0;
            _context->_inBuf->readData(&numElements, sizeof(numElements));
            _context->_inBuf->readData(&numBytes, sizeof(numBytes));

            captureOutView(_sink->beginNestedList(numElements, numBytes));

            return true;
        } else {
            // Fixed-width element: the wire layout [tag][value] is identical to the stored
            // layout, so copy it straight into the reserved slot — no intermediate value.
            const size_t elementBytes = tagSize + sizeof(T);
            if (_context->_inBuf->readable() < elementBytes) {
                return false;
            }

            captureOutView(_sink->writeListElementBytes(_context->_inBuf->readPtr(), elementBytes));
            _context->_inBuf->increaseReadOffset(elementBytes);

            return true;
        }
    }

private:
    // If a caller asked for it (the ListElementView columns, which store one view per row),
    // hands back the view of the element just written.
    void captureOutView(const SinkListElementView<Sink> view) const {
        if (_outView) {
            *_outView = view;
        }
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
template <ProtoDecodeSink Sink, typename OnTopLevelElement>
inline bool drainListStack(DecodeContext* context,
                           Sink* sink,
                           const OnTopLevelElement& onTopLevelElement) {
    SinkListElementView<Sink> view;
    const ListElementReadVisitor<Sink> readVisitor {context, sink, &view};

    while (sink->hasOpenList()) {
        if (sink->topListComplete()) {
            sink->popList();
            continue;
        }

        if (context->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
            return false;
        }

        db::ListBufferTypeTag tag {};
        // Peek the tag (don't consume): keeping it in the buffer lets the fixed-width path
        // memcpy [tag][value] in one go.
        memcpy(&tag, context->_inBuf->readPtr(), sizeof(tag));

        const bool topLevel = (sink->openListCount() == 1);
        const size_t elementIndex = sink->topLevelElementsWritten();

        const bool elementComplete = db::ListTagDispatcher {tag}.execute(readVisitor, db::ListElementView {});

        // In the special case of list element view where each element corresponds to a
        // container column row - onTopLevelElement is used to write the value into the
        // column
        if (topLevel && sink->topLevelElementsWritten() > elementIndex) {
            onTopLevelElement(elementIndex, view);
        }
        if (!elementComplete) {
            return false;
        }
    }

    return true;
}

// Per-wire-type column decoders. T comes from the wire type code (supplied explicitly by the
// dispatch layer).
template <typename T, ProtoDecodeSink Sink>
struct VectorColumnDecoder {
    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeVector");

        if (context->_rowIndex == 0) {
            typedColumn->resize(columnState->getNumRows());
        }

        const size_t rowsRemaining = columnState->getNumRows() - context->_rowIndex;

        if (rowsRemaining * sizeof(T) > context->_inBuf->readable()) {
            const size_t numRowsRead = context->_inBuf->readable() / sizeof(T);
            context->_inBuf->readData(typedColumn->data() + context->_rowIndex, numRowsRead * sizeof(T));
            context->_rowIndex += numRowsRead;
            return false;
        }

        context->_inBuf->readData(typedColumn->data() + context->_rowIndex, rowsRemaining * sizeof(T));
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<db::types::String::Primitive, Sink> {
    using T = db::types::String::Primitive;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        if (context->_rowIndex == 0) {
            typedColumn->reserve(columnState->getNumRows());
        }

        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                context->_rowIndex += i;
                return false;
            }

            WireSize stringSize = 0;
            context->_inBuf->readData(&stringSize, sizeof(stringSize));

            char* data = sink->allocString(stringSize);
            const T view = sink->getStringView(data, stringSize);

            typedColumn->emplace_back(view);
            if (!readVarLenPayload(context, data, stringSize)) {
                context->_rowIndex += i + 1;
                return false;
            }
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<SinkListView<Sink>, Sink> {
    using T = SinkListView<Sink>;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        size_t numRows = columnState->getNumRows();
        if (context->_rowIndex == 0 && typedColumn->size() == 0) {
            typedColumn->reserve(numRows);
        }

        while (context->_rowIndex < numRows) {
            // A list is "started" once we have read its header and emplaced its (initially
            // unfilled) ListView. typedColumn->size() vs _rowIndex tells us which: equal means the
            // current row's header is still to come.
            const bool listStarted = (typedColumn->size() == context->_rowIndex + 1);
            if (!listStarted) {
                if (context->_inBuf->readable() < 2 * sizeof(WireSize)) {
                    return false;
                }

                WireSize elementCount = 0;
                WireSize listByteSize = 0;
                context->_inBuf->readData(&elementCount, sizeof(elementCount));
                context->_inBuf->readData(&listByteSize, sizeof(listByteSize));

                typedColumn->emplace_back(sink->beginList(elementCount, listByteSize));
            }

            // Stream this row's list (and any nested children) straight into the reserved space.
            auto onTopLevelElement = [](size_t, const SinkListElementView<Sink>&) {};
            if (!drainListStack(context, sink, onTopLevelElement)) {
                return false;
            }

            ++context->_rowIndex;
        }

        return true;
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<SinkListElementView<Sink>, Sink> {
    using T = SinkListElementView<Sink>;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        // A ListElementView column has one element per column row, wire-encoded as
        // [listByteSize] followed by the elements. Each element is streamed onto the list
        // buffer and its view stored in the corresponding row.

        // If the row index is 0 this indicates that we haven't read any values from
        // our encoded ListView and need to allocate the list buffer itself
        if (context->_rowIndex == 0) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                return false;
            }

            WireSize listByteSize = 0;
            context->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            sink->beginList(columnState->getNumRows(), listByteSize);
            typedColumn->resize(columnState->getNumRows());
            context->_rowIndex = 1;
        }

        // One column row per top-level element; its view is stored in the matching row.
        auto onTopLevelElement = [typedColumn](size_t index, const SinkListElementView<Sink>& view) {
            typedColumn->data()[index] = view;
        };
        return drainListStack(context, sink, onTopLevelElement);
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<db::Path, Sink> {
    using T = db::Path;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        if (context->_rowIndex == 0) {
            typedColumn->reserve(columnState->getNumRows());
        }

        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                context->_rowIndex += i;
                return false;
            }

            WireSize pathByteSize = 0;
            context->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
            const size_t numEntities = checkedElementCount<db::EntityID>(pathByteSize, "path");

            auto& path = typedColumn->emplace_back(numEntities);
            if (!readVarLenPayload(context, reinterpret_cast<char*>(path.data()), pathByteSize)) {
                context->_rowIndex += i + 1;
                return false;
            }
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<db::types::Embedding::Primitive, Sink> {
    using T = db::types::Embedding::Primitive;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        if (context->_rowIndex == 0) {
            typedColumn->reserve(columnState->getNumRows());
        }

        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                context->_rowIndex += i;
                return false;
            }

            WireSize embeddingSize = 0;
            context->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
            const size_t numFloats = checkedElementCount<float>(embeddingSize, "embedding");

            auto* data = sink->allocEmbedding(numFloats);
            const std::span<const float> span = sink->getEmbeddingView(data, numFloats);

            typedColumn->emplace_back(span);
            if (!readVarLenPayload(context, reinterpret_cast<char*>(data), embeddingSize)) {
                context->_rowIndex += i + 1;
                return false;
            }
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct VectorColumnDecoder<db::EntityList, Sink> {
    using T = db::EntityList;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        if (context->_rowIndex == 0) {
            typedColumn->resize(columnState->getNumRows());
        }

        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            auto& entityList = (*typedColumn)[context->_rowIndex + i];

            if (!context->_entityListEntryCount.has_value()) {
                if (context->_inBuf->readable() < sizeof(WireSize)) {
                    context->_rowIndex += i;
                    return false;
                }

                WireSize numberOfEntries = 0;
                context->_inBuf->readData(&numberOfEntries, sizeof(numberOfEntries));
                context->_entityListEntryCount = numberOfEntries;
                entityList.reserve(numberOfEntries);
            }

            const size_t expectedEntries = *context->_entityListEntryCount;

            while (entityList.size() < expectedEntries) {
                if (context->_inBuf->readable() < sizeof(db::EntityList::Entry::_type)) {
                    context->_rowIndex += i;
                    return false;
                }

                entityList.add();

                context->_inBuf->readData(&entityList.back()._type, sizeof(db::EntityList::Entry::_type));

                if (context->_inBuf->readable() < sizeof(db::EntityList::Entry::_id)) {
                    throw TuringException("Entity List Entry can't be broken up across packet/buffer boundaries");
                }

                context->_inBuf->readData(&entityList.back()._id, sizeof(db::EntityList::Entry::_id));
            }

            context->_entityListEntryCount.reset();
        }

        return true;
    }
};

template <typename T, ProtoDecodeSink Sink>
struct OptionalVectorColumnDecoder {
    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnOptVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptVector");

        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(T)) {
                context->_rowIndex += i;
                return false;
            }

            auto& entry = (*typedColumn)[context->_rowIndex + i];

            const bool hasValue = columnState->getBitMask().test(context->_rowIndex + i);
            if (!hasValue) {
                context->_inBuf->increaseReadOffset(sizeof(T));
                continue;
            }

            T value {};
            context->_inBuf->readData(&value, sizeof(T));
            entry = value;
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct OptionalVectorColumnDecoder<SinkListElementView<Sink>, Sink> {
    using T = SinkListElementView<Sink>;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnOptVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        // One element per column row, as in the non-optional case, wire-encoded as
        // [listByteSize] followed by the elements. Which rows hold one is read off the mask,
        // not off the tag: a row holding a null out of a list carries the very same tag.
        if (context->_rowIndex == 0) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                return false;
            }

            WireSize listByteSize = 0;
            context->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            sink->beginList(columnState->getNumRows(), listByteSize);
            context->_rowIndex = 1;
        }

        const ProtoColumnState::BitMask& mask = columnState->getBitMask();
        auto onTopLevelElement = [typedColumn, &mask](size_t index, const SinkListElementView<Sink>& view) {
            if (mask.test(index)) {
                typedColumn->data()[index] = view;
            }
        };

        return drainListStack(context, sink, onTopLevelElement);
    }
};

template <ProtoDecodeSink Sink>
struct OptionalVectorColumnDecoder<db::types::String::Primitive, Sink> {
    using T = db::types::String::Primitive;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnOptVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                context->_rowIndex += i;
                return false;
            }

            auto& entry = (*typedColumn)[context->_rowIndex + i];

            const bool hasValue = columnState->getBitMask().test(context->_rowIndex + i);
            if (!hasValue) {
                context->_inBuf->increaseReadOffset(sizeof(WireSize));
                continue;
            }

            WireSize stringSize = 0;
            context->_inBuf->readData(&stringSize, sizeof(stringSize));

            char* data = sink->allocString(stringSize);
            const T view = sink->getStringView(data, stringSize);

            entry.emplace(view);
            if (!readVarLenPayload(context, data, stringSize)) {
                context->_rowIndex += i + 1;
                return false;
            }
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct OptionalVectorColumnDecoder<db::types::Embedding::Primitive, Sink> {
    using T = db::types::Embedding::Primitive;

    static bool decode(DecodeContext* context,
                       Sink* sink,
                       SinkColumnOptVector<T, Sink>* typedColumn,
                       ProtoColumnState* columnState) {
        for (size_t i = 0; context->_rowIndex + i < columnState->getNumRows(); ++i) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                context->_rowIndex += i;
                return false;
            }

            auto& entry = (*typedColumn)[context->_rowIndex + i];

            const bool hasValue = columnState->getBitMask().test(context->_rowIndex + i);
            if (!hasValue) {
                context->_inBuf->increaseReadOffset(sizeof(WireSize));
                continue;
            }

            WireSize embeddingSize = 0;
            context->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
            const size_t numFloats = checkedElementCount<float>(embeddingSize, "embedding");

            auto* data = sink->allocEmbedding(numFloats);
            const std::span<const float> span = sink->getEmbeddingView(data, numFloats);

            entry.emplace(span);
            if (!readVarLenPayload(context, reinterpret_cast<char*>(data), embeddingSize)) {
                context->_rowIndex += i + 1;
                return false;
            }
        }
        return true;
    }
};

// We only need one set of decoders for the const as the value flag is checked before calling
// these functions leaving the remaining functionality for const and optional consts the same.
template <typename T, ProtoDecodeSink Sink>
struct ConstColumnDecoder {
    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeConst");

        if (context->_inBuf->readable() < sizeof(T)) {
            return false;
        }

        T value {};
        context->_inBuf->readData(&value, sizeof(T));
        typedColumn->set(value);
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<db::types::String::Primitive, Sink> {
    using T = db::types::String::Primitive;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        if (context->_inBuf->readable() < sizeof(WireSize)) {
            return false;
        }

        WireSize stringSize = 0;
        context->_inBuf->readData(&stringSize, sizeof(stringSize));

        char* data = sink->allocString(stringSize);
        const T view = sink->getStringView(data, stringSize);

        typedColumn->set(view);
        if (!readVarLenPayload(context, data, stringSize)) {
            // A const column has no row dimension: draining the queued resume completes it,
            // so advance _columnIndex now or the resumed pass would re-enter this decoder
            // and read the next column's bytes as fresh framing.
            ++context->_columnIndex;
            context->_rowIndex = 0;
            return false;
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<db::Path, Sink> {
    using T = db::Path;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        if (context->_inBuf->readable() < sizeof(WireSize)) {
            return false;
        }

        WireSize pathByteSize = 0;
        context->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
        const size_t numEntities = checkedElementCount<db::EntityID>(pathByteSize, "path");

        db::Path path(numEntities);
        char* pathBytes = reinterpret_cast<char*>(path.data());

        // db::Path is a std::vector, so moving it into the column keeps pathBytes valid.
        typedColumn->set(std::move(path));
        if (!readVarLenPayload(context, pathBytes, pathByteSize)) {
            // See the string branch: completing the resume completes the column.
            ++context->_columnIndex;
            context->_rowIndex = 0;
            return false;
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<db::types::Embedding::Primitive, Sink> {
    using T = db::types::Embedding::Primitive;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        if (context->_inBuf->readable() < sizeof(WireSize)) {
            return false;
        }

        WireSize embeddingSize = 0;
        context->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
        const size_t numFloats = checkedElementCount<float>(embeddingSize, "embedding");

        auto* data = sink->allocEmbedding(numFloats);
        const std::span<const float> span = sink->getEmbeddingView(data, numFloats);

        typedColumn->set(span);
        if (!readVarLenPayload(context, reinterpret_cast<char*>(data), embeddingSize)) {
            // See the string branch: completing the resume completes the column.
            ++context->_columnIndex;
            context->_rowIndex = 0;
            return false;
        }
        return true;
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<SinkListView<Sink>, Sink> {
    using T = SinkListView<Sink>;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        // Const columns have no row dimension, so _constListStarted carries the "list
        // started" state (the driver clears it before this column). Set means we have read
        // the header, reserved the space, and set the (initially unfilled) ListView on the
        // column.
        if (!context->_constListStarted) {
            if (context->_inBuf->readable() < 2 * sizeof(WireSize)) {
                return false;
            }

            WireSize elementCount = 0;
            WireSize listByteSize = 0;
            context->_inBuf->readData(&elementCount, sizeof(elementCount));
            context->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            typedColumn->set(sink->beginList(elementCount, listByteSize));
            context->_constListStarted = true;
        }
        auto onTopLevelElement = [](size_t, const SinkListElementView<Sink>&) {};
        return drainListStack(context, sink, onTopLevelElement);
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<SinkListElementView<Sink>, Sink> {
    using T = SinkListElementView<Sink>;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        // A constant ListElementView is a single element, wire-encoded as [listByteSize] + one
        // element. Stream it onto the list buffer and set the column to its view.

        if (!context->_constListStarted) {
            if (context->_inBuf->readable() < sizeof(WireSize)) {
                return false;
            }

            WireSize listByteSize = 0;
            context->_inBuf->readData(&listByteSize, sizeof(listByteSize));

            sink->beginList(1, listByteSize);
            context->_constListStarted = true;
        }

        // The constant is the single top-level element; store its view on the column.
        auto onTopLevelElement = [typedColumn](size_t, const SinkListElementView<Sink>& view) {
            typedColumn->set(view);
        };

        return drainListStack(context, sink, onTopLevelElement);
    }
};

template <ProtoDecodeSink Sink>
struct ConstColumnDecoder<db::PropertyNull, Sink> {
    using T = db::PropertyNull;

    template <ConstColumnOf<T, Sink> Column>
    static bool decode(DecodeContext* context, Sink* sink, Column* typedColumn) {
        return true;
    }
};

}
