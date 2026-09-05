#include "EncodedList.h"

#include <string.h>

#include "ListBufferTypeTag.h"
#include "ListContainer.h"
#include "ListElementView.h"

#include "ID.h"
#include "metadata/PropertyNull.h"
#include "metadata/PropertyType.h"

#include "FatalException.h"

using namespace db;

namespace {

void appendBytes(std::vector<std::byte>& out, const void* data, size_t size) {
    const std::byte* first = static_cast<const std::byte*>(data);
    out.insert(out.end(), first, first + size);
}

template <typename T>
void appendValue(std::vector<std::byte>& out, const T& value) {
    appendBytes(out, &value, sizeof(T));
}

void encodeList(ListView list, std::vector<std::byte>& out);

void encodeElement(ListElementView element, std::vector<std::byte>& out) {
    const ListBufferTypeTag tag = element.getTag();
    appendValue(out, tag);

    switch (tag) {
        case ListBufferTypeTag::Int:
            appendValue(out, element.getAs<types::Int64::Primitive>());
        break;
        case ListBufferTypeTag::UInt:
            appendValue(out, element.getAs<types::UInt64::Primitive>());
        break;
        case ListBufferTypeTag::Double:
            appendValue(out, element.getAs<types::Double::Primitive>());
        break;
        case ListBufferTypeTag::Bool:
            appendValue(out, element.getAs<types::Bool::Primitive>());
        break;
        case ListBufferTypeTag::NodeID:
            appendValue(out, element.getAs<NodeID>());
        break;
        case ListBufferTypeTag::EdgeID:
            appendValue(out, element.getAs<EdgeID>());
        break;
        case ListBufferTypeTag::Null:
        break;
        case ListBufferTypeTag::String: {
            const types::String::Primitive value = element.getAs<types::String::Primitive>();
            appendValue<uint64_t>(out, value.size());
            appendBytes(out, value.data(), value.size());
        }
        break;
        case ListBufferTypeTag::Embedding: {
            const types::Embedding::Primitive value = element.getAs<types::Embedding::Primitive>();
            appendValue<uint64_t>(out, value.size());
            appendBytes(out, value.data(), value.size_bytes());
        }
        break;
        case ListBufferTypeTag::ListView:
            encodeList(element.getAs<ListView>(), out);
        break;
        case ListBufferTypeTag::INVALID:
            throw FatalException("Cannot encode a list element with an invalid type tag");
        break;
    }
}

void encodeList(ListView list, std::vector<std::byte>& out) {
    appendValue<uint64_t>(out, list.size());

    for (const ListElementView element : list) {
        encodeElement(element, out);
    }
}

/**
 * @brief Reads back what the functions above wrote, storing every list it walks - the
 * nested ones first - in the container the elements of the enclosing list then reference.
 */
class ListDecoder {
public:
    ListDecoder(std::span<const std::byte> bytes, ListContainer& container)
        : _bytes(bytes),
        _container(container)
    {
    }

    ListView decodeList() {
        const uint64_t count = read<uint64_t>();

        std::vector<ListContainer::ListItemVariant> elements;
        elements.reserve(count);

        for (uint64_t i = 0; i < count; i++) {
            elements.push_back(decodeElement());
        }

        return _container.insert(elements);
    }

private:
    std::span<const std::byte> _bytes;
    ListContainer& _container;
    size_t _offset {0};

    template <typename T>
    T read() {
        T value {};
        readBytes(&value, sizeof(T));
        return value;
    }

    void readBytes(void* destination, size_t size) {
        if (_offset + size > _bytes.size()) {
            throw FatalException("Truncated encoded list");
        }

        memcpy(destination, _bytes.data() + _offset, size);
        _offset += size;
    }

    /// Hands back a span of the encoded bytes themselves; the container copies the payload
    /// out of it as it stores the element
    std::span<const std::byte> readPayload(size_t size) {
        if (_offset + size > _bytes.size()) {
            throw FatalException("Truncated encoded list");
        }

        const std::span<const std::byte> payload = _bytes.subspan(_offset, size);
        _offset += size;
        return payload;
    }

    ListContainer::ListItemVariant decodeElement() {
        const ListBufferTypeTag tag = read<ListBufferTypeTag>();

        switch (tag) {
            case ListBufferTypeTag::Int:
                return read<types::Int64::Primitive>();
            break;
            case ListBufferTypeTag::UInt:
                return read<types::UInt64::Primitive>();
            break;
            case ListBufferTypeTag::Double:
                return read<types::Double::Primitive>();
            break;
            case ListBufferTypeTag::Bool:
                return read<types::Bool::Primitive>();
            break;
            case ListBufferTypeTag::NodeID:
                return read<NodeID>();
            break;
            case ListBufferTypeTag::EdgeID:
                return read<EdgeID>();
            break;
            case ListBufferTypeTag::Null:
                return PropertyNull {};
            break;
            case ListBufferTypeTag::String:
                return decodeString();
            break;
            case ListBufferTypeTag::Embedding:
                return decodeEmbedding();
            break;
            case ListBufferTypeTag::ListView:
                return decodeList();
            break;
            case ListBufferTypeTag::INVALID:
            break;
        }

        throw FatalException("Unknown type tag in encoded list");
    }

    types::String::Primitive decodeString() {
        const uint64_t size = read<uint64_t>();
        const std::span<const std::byte> payload = readPayload(size);

        return {reinterpret_cast<const char*>(payload.data()), size};
    }

    types::Embedding::Primitive decodeEmbedding() {
        const uint64_t count = read<uint64_t>();
        const std::span<const std::byte> payload = readPayload(count * sizeof(float));

        return {reinterpret_cast<const float*>(payload.data()), count};
    }
};

}

EncodedList::EncodedList() {
}

EncodedList::EncodedList(ListView list) {
    encodeList(list, _bytes);
}

EncodedList::EncodedList(std::span<const std::byte> bytes)
    : _bytes(bytes.begin(), bytes.end())
{
}

EncodedList::~EncodedList() {
}

EncodedList::EncodedList(const EncodedList& other)
    : _bytes(other._bytes)
{
}

EncodedList::EncodedList(EncodedList&& other) noexcept
    : _bytes(std::move(other._bytes))
{
}

EncodedList& EncodedList::operator=(const EncodedList& other) {
    _bytes = other._bytes;
    return *this;
}

EncodedList& EncodedList::operator=(EncodedList&& other) noexcept {
    _bytes = std::move(other._bytes);
    return *this;
}

ListView EncodedList::decodeInto(ListContainer& container) const {
    ListDecoder decoder(_bytes, container);
    return decoder.decodeList();
}
