#pragma once

#include <optional>
#include <span>
#include <stack>
#include <stdint.h>
#include <string_view>
#include <type_traits>

#include "OutputValues.h"
#include "NestedContainerIterator.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoOutBuf.h"
#include "list/ListUtils.h"
#include "map/MapUtils.h"

namespace net::proto {

/// Dispatched on a list element's runtime tag to return the size of the object
/// the tag maps to, so the client knows how much to allocate. For String and
/// Embedding this is the size of the view object (std::string_view / std::span),
/// not the length of the data it points to.
struct ListElementByteSizeVisitor {
    template <typename T>
    size_t operator()(const db::ListElementView elem) const {
        return sizeof(T);
    }
};

// Σ sizeof(value) over the elements — the deserialized footprint the decoder reserves.
inline WireSize computeListByteSize(std::span<const db::ListElementView> elements) {
    const ListElementByteSizeVisitor sizeVisitor;

    size_t totalSize = 0;
    for (const auto& elem : elements) {
        db::ListTagDispatcher dispatcher {elem.getTag()};
        totalSize += dispatcher.execute(sizeVisitor, elem);
    }

    bioassert(totalSize <= MAX_WIRE_SIZE, "List length exceeds maximum wire size");
    return static_cast<WireSize>(totalSize);
}

// The same for an optional column's elements, counting a row that holds no element as the
// null it is written as.
inline WireSize computeOptionalListByteSize(std::span<const std::optional<db::ListElementView>> elements) {
    const ListElementByteSizeVisitor sizeVisitor;

    size_t totalSize = 0;
    for (const std::optional<db::ListElementView>& element : elements) {
        if (!element.has_value()) {
            totalSize += sizeof(db::PropertyNull);
            continue;
        }

        db::ListTagDispatcher dispatcher {element->getTag()};
        totalSize += dispatcher.execute(sizeVisitor, *element);
    }

    bioassert(totalSize <= MAX_WIRE_SIZE, "List length exceeds maximum wire size");
    return static_cast<WireSize>(totalSize);
}

/// Dispatched on a map entry's runtime value tag to return the size of the object the tag maps
/// to, so the client knows how much to allocate. As for a list element, String and Embedding
/// give the size of the view object, not the length of the data it points to.
struct MapEntryByteSizeVisitor {
    template <typename T>
    size_t operator()(const db::MapEntryView entry) const {
        return sizeof(T);
    }
};

// Σ sizeof(value) over the entries — the deserialized footprint the decoder reserves. The keys
// are not counted: a key is stored as a std::string_view however long its characters are, so
// the decoder adds that fixed per-entry cost itself rather than trusting our sizeof for it.
inline WireSize computeMapByteSize(std::span<const db::MapEntryView> entries) {
    const MapEntryByteSizeVisitor sizeVisitor;

    size_t totalSize = 0;
    for (const db::MapEntryView entry : entries) {
        db::MapTagDispatcher dispatcher {entry.getValueTag()};
        totalSize += dispatcher.execute(sizeVisitor, entry);
    }

    bioassert(totalSize <= MAX_WIRE_SIZE, "Map length exceeds maximum wire size");
    return static_cast<WireSize>(totalSize);
}

// Structs describing policies we can pass to @ElementWriteVisitor to describe how it
// should write values belonging to their respective containeres
struct ListWritePolicy {
    using View = db::ListElementView;
    using Tag = db::ListBufferTypeTag;

    template <typename T>
    static constexpr Tag tagFor() { return db::TypeToListBufferTag<T>::Tag; }

    template <typename T>
    static T valueAs(View view) { return view.getAs<T>(); }
};

struct MapWritePolicy {
    using View = db::MapEntryView;
    using Tag = db::MapBufferTypeTag;

    template <typename T>
    static constexpr Tag tagFor() { return db::TypeToMapBufferTag<T>::Tag; }

    template <typename T>
    static T valueAs(View view) { return view.getValueAs<T>(); }
};

/// Dispatched on a value's runtime tag to write [tag][value] for fixed types, or
/// [tag][numBytes][data] for variable-length types (String, Embedding). The tag and the
/// fixed framing are kept within a single chunk (checkRemainingAndFlush) so the decoder,
/// which reads each unit atomically, never sees them split across a packet boundary; the
/// variable payload that follows still streams across chunks via copyVarLenData.
/// A map entry's key is written by the drain loop before the value reaches this visitor.
template <typename Policy>
struct ElementWriteVisitor {
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    NestedContainerIterator::Stack* _stack {nullptr};

    template <typename T>
    void operator()(const typename Policy::View view) const {
        using Tag = typename Policy::Tag;

        constexpr size_t tagSize = sizeof(Tag);
        const Tag tag = Policy::template tagFor<T>();

        if constexpr (db::StringLike<T>) {
            const T value = Policy::template valueAs<T>(view);
            bioassert(value.size() * sizeof(char) <= MAX_WIRE_SIZE, "String element exceeds maximum wire size");
            const WireSize numBytes = static_cast<WireSize>(value.size() * sizeof(char));

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(numBytes));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&numBytes, sizeof(numBytes));
            _outBuf->copyVarLenData(value.data(), numBytes);
        } else if constexpr (db::IsEmbedding<T>) {
            const T value = Policy::template valueAs<T>(view);
            bioassert(value.size() * sizeof(float) <= MAX_WIRE_SIZE, "Embedding element exceeds maximum wire size");
            const WireSize numBytes = static_cast<WireSize>(value.size() * sizeof(float));

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(numBytes));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&numBytes, sizeof(numBytes));
            _outBuf->copyVarLenData(value.data(), numBytes);
        } else if constexpr (db::IsListView<T>) {
            const T value = Policy::template valueAs<T>(view);

            bioassert(value.size() <= MAX_WIRE_SIZE, "List element count exceeds maximum wire size");
            const WireSize elementCount = static_cast<WireSize>(value.size());
            const WireSize listByteSize = computeListByteSize(value.elements());

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(elementCount) + sizeof(listByteSize));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&elementCount, sizeof(elementCount));
            _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

            _stack->emplace(NestedContainerIterator::list(value.elements()));
        } else if constexpr (db::IsMap<T>) {
            const T value = Policy::template valueAs<T>(view);

            bioassert(value.size() <= MAX_WIRE_SIZE, "Map entry count exceeds maximum wire size");
            const WireSize entryCount = static_cast<WireSize>(value.size());
            const WireSize mapByteSize = computeMapByteSize(value.entries());

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(entryCount) + sizeof(mapByteSize));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&entryCount, sizeof(entryCount));
            _outBuf->copyFixedLenData(&mapByteSize, sizeof(mapByteSize));

            _stack->emplace(NestedContainerIterator::map(value.entries()));
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "ElementWriteVisitor can't encode a non trivial element in a trivial manner");
            const T value = Policy::template valueAs<T>(view);

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(T));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&value, sizeof(T));
        }
    }
};

/**
 * @brief Writes one nested container — a list or a map, up to any depth — onto the wire.
 *
 * Hold a stack that we use to store parent containers as we finish encoding and writing
 * the child containers.
 */
class NestedContainerWriter {
public:
    explicit NestedContainerWriter(net::proto::TuringProtoOutBuf* outBuf);
    ~NestedContainerWriter();

    // A whole list value: [count][listByteSize] followed by the elements.
    void writeListView(const db::ListView& listView);
    // A whole map value: [entryCount][mapByteSize] followed by its [keyLen][keyBytes][tag][value]
    // entries.
    void writeMapView(const db::MapView& mapView);

    // A column whose rows are each one list element: [listByteSize] then the elements, the
    // row count having already given their number.
    void writeListElements(std::span<const db::ListElementView> elements);

    // The same for an optional column. A row holding no element is written as a null one so
    // the elements stay one per row; the column's null mask, already on the wire, is what
    // tells such a row from one holding a null read out of a list.
    void writeOptionalListElements(std::span<const std::optional<db::ListElementView>> elements);

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    NestedContainerIterator::Stack _stack;

    void drainNestedValues();
    void writeListElementValues(std::span<const db::ListElementView> elements);
    void writeMapKey(std::string_view key);
    void writeNullListElement();
};

}
