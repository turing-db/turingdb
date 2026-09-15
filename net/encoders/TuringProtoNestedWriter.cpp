#include "TuringProtoNestedWriter.h"

using namespace net::proto;

NestedContainerWriter::NestedContainerWriter(net::proto::TuringProtoOutBuf* outBuf)
    : _outBuf(outBuf)
{
}

NestedContainerWriter::~NestedContainerWriter() {
}

void NestedContainerWriter::writeListView(const db::ListView& listView) {
    const std::span<const db::ListElementView> elements = listView.elements();

    bioassert(elements.size() <= MAX_WIRE_SIZE, "List element count exceeds maximum wire size");
    const WireSize elementCount = static_cast<WireSize>(elements.size());
    const WireSize listByteSize = computeListByteSize(elements);

    // Keep the [count][listByteSize] header together in one chunk.
    _outBuf->checkRemainingAndFlush(sizeof(elementCount) + sizeof(listByteSize));
    _outBuf->copyFixedLenData(&elementCount, sizeof(elementCount));
    _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

    writeListElementValues(elements);
}

void NestedContainerWriter::writeMapView(const db::MapView& mapView) {
    const std::span<const db::MapEntryView> entries = mapView.entries();

    bioassert(entries.size() <= MAX_WIRE_SIZE, "Map entry count exceeds maximum wire size");
    const WireSize entryCount = static_cast<WireSize>(entries.size());
    const WireSize mapByteSize = computeMapByteSize(entries);

    // Keep the [entryCount][mapByteSize] header together in one chunk.
    _outBuf->checkRemainingAndFlush(sizeof(entryCount) + sizeof(mapByteSize));
    _outBuf->copyFixedLenData(&entryCount, sizeof(entryCount));
    _outBuf->copyFixedLenData(&mapByteSize, sizeof(mapByteSize));

    _stack.emplace(NestedContainerIterator::map(entries));
    drainNestedValues();
}

void NestedContainerWriter::writeListElements(std::span<const db::ListElementView> elements) {
    const WireSize listByteSize = computeListByteSize(elements);

    _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

    writeListElementValues(elements);
}

void NestedContainerWriter::writeOptionalListElements(std::span<const std::optional<db::ListElementView>> elements) {
    const WireSize listByteSize = computeOptionalListByteSize(elements);

    _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

    for (const std::optional<db::ListElementView>& element : elements) {
        if (!element.has_value()) {
            writeNullListElement();
            continue;
        }

        writeListElementValues(std::span<const db::ListElementView>(&element.value(), 1));
    }
}

// Drain the stack of nested values. A nested container will push a nested frame onto the stack
// which will need to be fully drained and popped off the stack before the parent frame continues draining.
void NestedContainerWriter::drainNestedValues() {
    const ElementWriteVisitor<ListWritePolicy> listVisitor {_outBuf, &_stack};
    const ElementWriteVisitor<MapWritePolicy> mapVisitor {_outBuf, &_stack};

    while (!_stack.empty()) {
        NestedContainerIterator& frame = _stack.top();

        if (frame.isExhausted()) {
            _stack.pop();
            continue;
        }

        if (frame.isMap()) {
            const db::MapEntryView entry = frame.getEntry();

            writeMapKey(entry.getKey());
            db::MapTagDispatcher {entry.getValueTag()}.execute(mapVisitor, entry);
        } else {
            const db::ListElementView element = frame.getElement();

            db::ListTagDispatcher {element.getTag()}.execute(listVisitor, element);
        }

        frame.advance();
    }
}

void NestedContainerWriter::writeListElementValues(std::span<const db::ListElementView> elements) {
    _stack.emplace(NestedContainerIterator::list(elements));
    drainNestedValues();
}

void NestedContainerWriter::writeMapKey(std::string_view key) {
    bioassert(key.size() <= MAX_WIRE_SIZE, "Map key length exceeds maximum wire size");
    const WireSize keyLen = static_cast<WireSize>(key.size());

    _outBuf->copyFixedLenData(&keyLen, sizeof(keyLen));
    _outBuf->copyVarLenData(key.data(), keyLen);
}

void NestedContainerWriter::writeNullListElement() {
    constexpr size_t tagSize = sizeof(db::ListBufferTypeTag);
    constexpr db::ListBufferTypeTag tag = db::TypeToListBufferTag<db::PropertyNull>::Tag;
    const db::PropertyNull value {};

    _outBuf->checkRemainingAndFlush(tagSize + sizeof(value));
    _outBuf->copyFixedLenData(&tag, tagSize);
    _outBuf->copyFixedLenData(&value, sizeof(value));
}
