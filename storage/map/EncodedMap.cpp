#include "EncodedMap.h"

#include <string.h>

#include <algorithm>

#include "MapBufferTypeTag.h"
#include "MapContainer.h"
#include "MapEntryView.h"

#include "ID.h"
#include "list/EncodedList.h"
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

void encodeMap(MapView map, std::vector<std::byte>& out);

void encodeValue(MapEntryView entry, std::vector<std::byte>& out) {
    const MapBufferTypeTag tag = entry.getValueTag();
    appendValue(out, tag);

    switch (tag) {
        case MapBufferTypeTag::Int:
            appendValue(out, entry.getValueAs<types::Int64::Primitive>());
        break;
        case MapBufferTypeTag::UInt:
            appendValue(out, entry.getValueAs<types::UInt64::Primitive>());
        break;
        case MapBufferTypeTag::Double:
            appendValue(out, entry.getValueAs<types::Double::Primitive>());
        break;
        case MapBufferTypeTag::Bool:
            appendValue(out, entry.getValueAs<types::Bool::Primitive>());
        break;
        case MapBufferTypeTag::NodeID:
            appendValue(out, entry.getValueAs<NodeID>());
        break;
        case MapBufferTypeTag::EdgeID:
            appendValue(out, entry.getValueAs<EdgeID>());
        break;
        case MapBufferTypeTag::DateTime:
            appendValue(out, entry.getValueAs<types::DateTime::Primitive>());
        break;
        case MapBufferTypeTag::Null:
        break;
        case MapBufferTypeTag::String: {
            const types::String::Primitive value = entry.getValueAs<types::String::Primitive>();
            appendValue<uint64_t>(out, value.size());
            appendBytes(out, value.data(), value.size());
        }
        break;
        case MapBufferTypeTag::Embedding: {
            const types::Embedding::Primitive value = entry.getValueAs<types::Embedding::Primitive>();
            appendValue<uint64_t>(out, value.size());
            appendBytes(out, value.data(), value.size_bytes());
        }
        break;
        case MapBufferTypeTag::ListView: {
            const EncodedList list(entry.getValueAs<ListView>());
            appendValue<uint64_t>(out, list.byteSize());
            appendBytes(out, list.bytes().data(), list.byteSize());
        }
        break;
        case MapBufferTypeTag::MapView:
            encodeMap(entry.getValueAs<MapView>(), out);
        break;
        case MapBufferTypeTag::INVALID:
            throw FatalException("Cannot encode a map entry with an invalid type tag");
        break;
    }
}

void encodeEntries(std::span<const MapEntryView> entries, std::vector<std::byte>& out) {
    appendValue<uint64_t>(out, entries.size());

    for (const MapEntryView entry : entries) {
        const std::string_view key = entry.getKey();
        appendValue<uint64_t>(out, key.size());
        appendBytes(out, key.data(), key.size());

        encodeValue(entry, out);
    }
}

void encodeMap(MapView map, std::vector<std::byte>& out) {
    const auto keyLess = [](const MapEntryView lhs, const MapEntryView rhs) {
        return lhs.getKey() < rhs.getKey();
    };

    const std::span<const MapEntryView> entries = map.entries();
    if (std::ranges::is_sorted(entries, keyLess)) {
        encodeEntries(entries, out);
    } else {
        std::vector<MapEntryView> sorted(entries.begin(), entries.end());
        std::ranges::stable_sort(sorted, keyLess);

        encodeEntries(sorted, out);
    }
}

/**
 * @brief Reads back what the functions above wrote, storing every map it walks - the
 * nested ones first - in the container the entries of the enclosing map then reference.
 */
class MapDecoder {
public:
    MapDecoder(std::span<const std::byte> bytes, MapContainer& container)
        : _bytes(bytes),
        _container(container)
    {
    }

    MapView decodeMap() {
        const uint64_t count = read<uint64_t>();

        std::vector<MapContainer::MapKeyValuePair> entries;
        entries.reserve(count);

        for (uint64_t i = 0; i < count; i++) {
            const std::string_view key = decodeString();
            entries.push_back({key, decodeValue()});
        }

        return _container.insert(entries);
    }

private:
    std::span<const std::byte> _bytes;
    MapContainer& _container;
    size_t _offset {0};

    template <typename T>
    T read() {
        T value {};
        readBytes(&value, sizeof(T));
        return value;
    }

    void readBytes(void* destination, size_t size) {
        if (_offset + size > _bytes.size()) {
            throw FatalException("Truncated encoded map");
        }

        memcpy(destination, _bytes.data() + _offset, size);
        _offset += size;
    }

    std::span<const std::byte> readPayload(size_t size) {
        if (_offset + size > _bytes.size()) {
            throw FatalException("Truncated encoded map");
        }

        const std::span<const std::byte> payload = _bytes.subspan(_offset, size);
        _offset += size;
        return payload;
    }

    MapBuffer<>::MapItemVariant decodeValue() {
        const MapBufferTypeTag tag = read<MapBufferTypeTag>();

        switch (tag) {
            case MapBufferTypeTag::Int:
                return read<types::Int64::Primitive>();
            break;
            case MapBufferTypeTag::UInt:
                return read<types::UInt64::Primitive>();
            break;
            case MapBufferTypeTag::Double:
                return read<types::Double::Primitive>();
            break;
            case MapBufferTypeTag::Bool:
                return read<types::Bool::Primitive>();
            break;
            case MapBufferTypeTag::NodeID:
                return read<NodeID>();
            break;
            case MapBufferTypeTag::EdgeID:
                return read<EdgeID>();
            break;
            case MapBufferTypeTag::DateTime:
                return read<types::DateTime::Primitive>();
            break;
            case MapBufferTypeTag::Null:
                return PropertyNull {};
            break;
            case MapBufferTypeTag::String:
                return decodeString();
            break;
            case MapBufferTypeTag::Embedding:
                return decodeEmbedding();
            break;
            case MapBufferTypeTag::ListView:
                return decodeList();
            break;
            case MapBufferTypeTag::MapView:
                return decodeMap();
            break;
            case MapBufferTypeTag::INVALID:
            break;
        }

        throw FatalException("Unknown type tag in encoded map");
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

    ListView decodeList() {
        const uint64_t size = read<uint64_t>();
        const EncodedList list(readPayload(size));

        return list.decodeInto(_container.getLists());
    }
};

}

EncodedMap::EncodedMap() {
}

EncodedMap::EncodedMap(MapView map) {
    encodeMap(map, _bytes);
}

EncodedMap::EncodedMap(std::span<const std::byte> bytes)
    : _bytes(bytes.begin(), bytes.end())
{
}

EncodedMap::~EncodedMap() {
}

EncodedMap::EncodedMap(const EncodedMap& other)
    : _bytes(other._bytes)
{
}

EncodedMap::EncodedMap(EncodedMap&& other) noexcept
    : _bytes(std::move(other._bytes))
{
}

EncodedMap& EncodedMap::operator=(const EncodedMap& other) {
    _bytes = other._bytes;
    return *this;
}

EncodedMap& EncodedMap::operator=(EncodedMap&& other) noexcept {
    _bytes = std::move(other._bytes);
    return *this;
}

MapView EncodedMap::decodeInto(MapContainer& container) const {
    MapDecoder decoder(_bytes, container);
    return decoder.decodeMap();
}

bool db::operator==(const EncodedMap& lhs, const EncodedMap& rhs) {
    return lhs.bytes().size() == rhs.bytes().size()
        && memcmp(lhs.bytes().data(), rhs.bytes().data(), lhs.bytes().size()) == 0;
}
