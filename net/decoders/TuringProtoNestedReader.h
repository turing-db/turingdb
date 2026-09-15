#pragma once

#include <stddef.h>
#include <cstring>

#include "DecodeContext.h"
#include "DecodeUtils.h"
#include "ProtoDecodeSink.h"
#include "NestedContainerKind.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "list/ListUtils.h"
#include "map/MapUtils.h"

namespace net::proto {

// The view each container's tag dispatcher hands its visitor, and the width of the tag it
// stores.
template <NestedContainerKind Kind>
struct ContainerViewOf {
    using Type = db::ListElementView;
};

template <>
struct ContainerViewOf<NestedContainerKind::Map> {
    using Type = db::MapEntryView;
};

template <NestedContainerKind Kind>
constexpr size_t tagSizeOf() {
    if constexpr (Kind == NestedContainerKind::List) {
        return sizeof(db::ListBufferTypeTag);
    } else {
        return sizeof(db::MapBufferTypeTag);
    }
}

/**
 * @brief Reads one value off the wire and appends it to the container on top of the sink's
 * stack, dispatched on the value's tag via @ref db::ListTagDispatcher or
 * @ref db::MapTagDispatcher.
 *
 * The tag is glanced (not yet consumed) by the caller; this visitor consumes the tag
 * together with its value once enough is buffered. Returns true if the value was fully
 * consumed, false if the read buffer ran dry: either before the value could start (nothing
 * consumed, retried next time) or partway through a String/Embedding payload (the value is
 * already appended and its bytes resume via @ref _bufferState).
 *
 * Both containers reserve their space up front, so a fixed-width value's wire layout
 * [tag][value] is copied straight into the reserved slot. In a map that slot sits after the
 * key's string_view, which the entry's key wrote before the value arrived.
 */
template <ProtoDecodeSink Sink, NestedContainerKind Kind>
struct NestedValueReadVisitor {
    DecodeContext* _context {nullptr};
    Sink* _sink {nullptr};
    // Optional: if set, receives the view of each element appended to the list buffer
    // (used by the ListElementView columns, which store one view per row).
    SinkListElementView<Sink>* _outView {nullptr};

    template <typename T>
    bool operator()(const typename ContainerViewOf<Kind>::Type unusedView) const {
        constexpr size_t tagSize = tagSizeOf<Kind>();

        if constexpr (std::is_same_v<T, db::types::String::Primitive>) {
            if (_context->_inBuf->readable() < tagSize + sizeof(WireSize)) {
                return false;
            }
            _context->_inBuf->increaseReadOffset(tagSize);

            WireSize numBytes = 0;
            _context->_inBuf->readData(&numBytes, sizeof(numBytes));

            char* dest = _sink->allocString(numBytes);
            const T view = _sink->getStringView(dest, numBytes);
            writeValue(view);

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
            writeValue(view);

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
        } else if constexpr (std::is_same_v<T, db::MapView>) {
            if (_context->_inBuf->readable() < tagSize + sizeof(WireSize) + sizeof(WireSize)) {
                return false;
            }
            _context->_inBuf->increaseReadOffset(tagSize);

            WireSize numEntries = 0;
            WireSize numBytes = 0;
            _context->_inBuf->readData(&numEntries, sizeof(numEntries));
            _context->_inBuf->readData(&numBytes, sizeof(numBytes));

            _sink->beginNestedMap(numEntries, numBytes);

            return true;
        } else {
            // The wire layout [tag][value] is identical to the stored layout, so copy it
            // straight into the reserved slot — no intermediate value.
            const size_t elementBytes = tagSize + sizeof(T);
            if (_context->_inBuf->readable() < elementBytes) {
                return false;
            }

            writeValueBytes(_context->_inBuf->readPtr(), elementBytes);
            _context->_inBuf->increaseReadOffset(elementBytes);

            return true;
        }
    }

private:
    // A list element is written through the column's cursor; a map value fills the entry
    // whose key has already been read.
    template <typename T>
    void writeValue(const T& value) const {
        if constexpr (Kind == NestedContainerKind::List) {
            captureOutView(_sink->writeListValue(value));
        } else {
            _sink->writeMapValue(value);
        }
    }

    // The same, for a [tag][value] pair lifted straight off the wire.
    void writeValueBytes(const char* bytes, size_t numBytes) const {
        if constexpr (Kind == NestedContainerKind::List) {
            captureOutView(_sink->writeListElementBytes(bytes, numBytes));
        } else {
            _sink->writeMapValueBytes(bytes, numBytes);
        }
    }

    // If a caller asked for it (the ListElementView columns, which store one view per row),
    // hands back the view of the element just written.
    void captureOutView(const SinkListElementView<Sink> view) const {
        if (_outView) {
            *_outView = view;
        }
    }
};

/**
 * @brief Reads one map entry's [keyLen][keyBytes] and parks the key on the open map, so the
 * value that follows fills the entry it opened.
 *
 * The key is handed to the sink before its bytes are streamed, exactly as a list's string
 * element is: that way a buffer running dry mid-key leaves the map already expecting a
 * value, and the resumed pass reads the value rather than the key again.
 */
template <ProtoDecodeSink Sink>
inline bool readMapKey(DecodeContext* context, Sink* sink) {
    if (context->_inBuf->readable() < sizeof(WireSize)) {
        return false;
    }

    WireSize keyLen = 0;
    context->_inBuf->readData(&keyLen, sizeof(keyLen));

    char* dest = sink->allocString(keyLen);
    sink->writeMapKey(sink->getStringView(dest, keyLen));

    return readVarLenPayload(context, dest, keyLen);
}

/**
 * @brief Closes every container whose last value has been written, even when that value's
 * bytes are still in flight.
 *
 * A container holds view objects, not the bytes they name, so it is finished the moment its
 * last value is recorded — the payload streams on into the arena the view points at. A map
 * is only materialized when it closes, so deferring that to the next decode pass would lose
 * it: the pass never comes when the payload's final bytes leave the read buffer empty.
 */
template <ProtoDecodeSink Sink>
inline void closeCompletedContainers(Sink* sink) {
    while (sink->hasOpenContainer() && sink->topContainerComplete()) {
        sink->popContainer();
    }
}

/**
 * @brief Drains the sink's open-container stack, reading values off the wire until the stack
 * empties (the whole top-level value is decoded) or the read buffer runs dry (returns false;
 * the stack and @ref _bufferState carry the resume point).
 *
 * For each top-level element completed (one written through the column's own cursor, i.e. stack
 * depth 1 — nested values live deeper and are not column rows), @param onTopLevelElement is
 * invoked with its 0-based index and view. ListElementView columns store the view; ListView
 * and MapView columns pass a no-op.
 *
 * return false if we have an incomplete value in the input buffer.
 */
template <ProtoDecodeSink Sink, typename OnTopLevelElement>
inline bool drainContainerStack(DecodeContext* context,
                                Sink* sink,
                                const OnTopLevelElement& onTopLevelElement) {
    SinkListElementView<Sink> view;
    const NestedValueReadVisitor<Sink, NestedContainerKind::List> listVisitor {context, sink, &view};
    const NestedValueReadVisitor<Sink, NestedContainerKind::Map> mapVisitor {context, sink, &view};

    while (sink->hasOpenContainer()) {
        if (sink->topContainerComplete()) {
            sink->popContainer();
            continue;
        }

        if (sink->topContainerIsMap()) {
            if (!sink->topMapExpectsValue() && !readMapKey(context, sink)) {
                return false;
            }

            if (context->_inBuf->readable() < sizeof(db::MapBufferTypeTag)) {
                return false;
            }

            db::MapBufferTypeTag tag {};
            memcpy(&tag, context->_inBuf->readPtr(), sizeof(tag));

            if (!db::MapTagDispatcher {tag}.execute(mapVisitor, db::MapEntryView {})) {
                // The value's bytes are still streaming, but its view is recorded, so every
                // container it completed can close. There is nothing left to drain once they
                // all have — the payload finishes through _bufferState on its own.
                closeCompletedContainers(sink);
                return !sink->hasOpenContainer();
            }

            continue;
        } else {
            if (context->_inBuf->readable() < sizeof(db::ListBufferTypeTag)) {
                return false;
            }

            db::ListBufferTypeTag tag {};
            // Peek the tag (don't consume): keeping it in the buffer lets the fixed-width path
            // memcpy [tag][value] in one go.
            memcpy(&tag, context->_inBuf->readPtr(), sizeof(tag));

            const bool topLevel = (sink->openContainerCount() == 1);
            const size_t elementIndex = sink->topLevelValuesWritten();

            const bool elementComplete = db::ListTagDispatcher {tag}.execute(listVisitor, db::ListElementView {});

            // In the special case of list element view where each element corresponds to a
            // container column row - onTopLevelElement is used to write the value into the
            // column
            if (topLevel && sink->topLevelValuesWritten() > elementIndex) {
                onTopLevelElement(elementIndex, view);
            }
            if (!elementComplete) {
                closeCompletedContainers(sink);
                return !sink->hasOpenContainer();
            }
        }
    }

    return true;
}

}
