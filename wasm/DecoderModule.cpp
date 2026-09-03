#include <stdint.h>
#include <string.h>
#include <array>
#include <optional>
#include <string>
#include <vector>

#include <emscripten/bind.h>
#include <emscripten/val.h>

#include "TuringProtoDecoder.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringException.h"

#include "WasmSink.h"

using emscripten::val;
using namespace net::proto;

namespace {

// The fixed-width element sizes the JS reader assumes per wire type.
static_assert(sizeof(db::types::Bool::Primitive) == 1);
static_assert(sizeof(db::ValueType) == 1);
static_assert(sizeof(db::PropertyNull) == 1);
static_assert(sizeof(db::PropertyTypeID) == 2);
static_assert(sizeof(db::LabelSetID) == 4);
static_assert(sizeof(db::EntityID) == 8);
static_assert(sizeof(db::ChangeID) == 8);

template <typename T>
constexpr bool isFixedWidth = TrivialInternalTypes<T> || std::is_same_v<T, db::PropertyNull>;

template <typename T>
constexpr bool isListHandle = std::is_same_v<T, wasm::ListView> || std::is_same_v<T, wasm::ListElementView>;

template <typename T>
constexpr bool isString = std::is_same_v<T, db::types::String::Primitive>;

struct ExportBuffers {
    std::vector<char> _values;
    std::vector<uint32_t> _offsets;
    std::vector<uint32_t> _utf16Offsets;
    std::vector<uint8_t> _validity;

    void clear() {
        _values.clear();
        _offsets.clear();
        _utf16Offsets.clear();
        _validity.clear();
    }
};

val copyToUint8Array(const void* bytes, size_t byteSize) {
    if (byteSize == 0) {
        return val::global("Uint8Array").new_(0);
    }

    return val::global("Uint8Array").new_(val(emscripten::typed_memory_view(byteSize, static_cast<const uint8_t*>(bytes))));
}

template <typename T>
val copyToUint8Array(const std::vector<T>& values) {
    return copyToUint8Array(values.data(), values.size() * sizeof(T));
}

size_t utf16Length(std::string_view text) {
    size_t length = 0;

    for (const unsigned char byte : text) {
        if ((byte & 0xC0) != 0x80) {
            ++length;
        }
        if (byte >= 0xF0) {
            ++length;
        }
    }

    return length;
}

template <typename T>
void appendVariableWidthItem(ExportBuffers& out, const T& item) {
    if constexpr (isString<T>) {
        out._values.insert(out._values.end(), item.begin(), item.end());
    } else if constexpr (std::is_same_v<T, db::types::Embedding::Primitive>) {
        const char* bytes = reinterpret_cast<const char*>(item.data());
        out._values.insert(out._values.end(), bytes, bytes + item.size_bytes());
    } else if constexpr (std::is_same_v<T, db::Path>) {
        const char* bytes = reinterpret_cast<const char*>(item.data());
        out._values.insert(out._values.end(), bytes, bytes + item.size() * sizeof(db::EntityID));
    } else if constexpr (std::is_same_v<T, db::EntityList>) {
        for (const db::EntityList::Entry& entry : item) {
            const uint64_t id = entry._id.getValue();
            const char* idBytes = reinterpret_cast<const char*>(&id);

            out._values.push_back(static_cast<char>(entry._type));
            out._values.insert(out._values.end(), idBytes, idBytes + sizeof(id));
        }
    } else {
        static_assert(sizeof(T) == 0, "No export layout for this element type");
    }
}

// A null item contributes zero bytes to a fixed-width column and an empty range to a
// variable-width one.
template <typename T>
void appendItem(ExportBuffers& out, const T* item) {
    if constexpr (isFixedWidth<T>) {
        const size_t offset = out._values.size();
        out._values.resize(offset + sizeof(T));

        if (item) {
            memcpy(out._values.data() + offset, item, sizeof(T));
        }
    } else if constexpr (isListHandle<T>) {
        out._offsets.push_back(item ? item->_offset : 0);
    } else {
        if (item) {
            appendVariableWidthItem(out, *item);
        }
        out._offsets.push_back(static_cast<uint32_t>(out._values.size()));

        if constexpr (isString<T>) {
            const uint32_t previous = out._utf16Offsets.back();
            out._utf16Offsets.push_back(previous + (item ? static_cast<uint32_t>(utf16Length(*item)) : 0));
        }
    }
}

// Builds the JS descriptor object of one column: type code, encoding, item count and fresh
// Uint8Array copies of its buffers (values, offsets, utf16Offsets, validity), each
// present only where the element kind and encoding call for it.
template <typename T, typename GetItem>
val exportItems(ExportBuffers& out, ColumnInternalKind typeCode, ColumnKind encoding, size_t itemCount, const GetItem& getItem) {
    out.clear();

    const bool optional = encoding == ColumnKind::OPTIONAL_VECTOR || encoding == ColumnKind::OPTIONAL_CONSTANT;

    if constexpr (isFixedWidth<T>) {
        out._values.reserve(itemCount * sizeof(T));
    } else if constexpr (!isListHandle<T>) {
        out._offsets.push_back(0);
        if constexpr (isString<T>) {
            out._utf16Offsets.push_back(0);
        }
    }

    if (optional) {
        out._validity.assign((itemCount + 7) / 8, 0);
    }

    for (size_t index = 0; index < itemCount; ++index) {
        const T* item = getItem(index);

        if (item && optional) {
            out._validity[index / 8] |= static_cast<uint8_t>(1u << (index % 8));
        }
        appendItem<T>(out, item);
    }

    val descriptor = val::object();
    descriptor.set("typeCode", static_cast<uint32_t>(typeCode));
    descriptor.set("encoding", static_cast<uint32_t>(encoding));
    descriptor.set("count", static_cast<uint32_t>(itemCount));

    if constexpr (isFixedWidth<T>) {
        descriptor.set("elementSize", static_cast<uint32_t>(sizeof(T)));
        descriptor.set("values", copyToUint8Array(out._values));
    } else if constexpr (isListHandle<T>) {
        descriptor.set("offsets", copyToUint8Array(out._offsets));
    } else {
        descriptor.set("values", copyToUint8Array(out._values));
        descriptor.set("offsets", copyToUint8Array(out._offsets));
        if constexpr (isString<T>) {
            descriptor.set("utf16Offsets", copyToUint8Array(out._utf16Offsets));
        }
    }

    if (optional) {
        descriptor.set("validity", copyToUint8Array(out._validity));
    }

    return descriptor;
}

// Take a decoded wasm column and use the functor to return a JS Descriptor object
struct ColumnBuffersFn {
    wasm::Column* _column {nullptr};
    ExportBuffers* _buffers {nullptr};

    template <typename T>
    val operator()(ColumnKind encoding) const {
        const ColumnInternalKind typeCode = _column->getInternalType();

        switch (encoding) {
            case ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T, wasm::WasmSink>) {
                    wasm::ColumnVector<T>* column = static_cast<wasm::ColumnVector<T>*>(_column);
                    const auto getItem = [column](size_t index) -> const T* { return &(*column)[index]; };
                    return exportItems<T>(*_buffers, typeCode, encoding, column->size(), getItem);
                } else {
                    throw TuringException("Unsupported type for Vector");
                }
            }
            break;
            case ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T, wasm::WasmSink>) {
                    wasm::ColumnVector<std::optional<T>>* column = static_cast<wasm::ColumnVector<std::optional<T>>*>(_column);
                    const auto getItem = [column](size_t index) -> const T* {
                        const std::optional<T>& value = (*column)[index];
                        return value.has_value() ? &*value : nullptr;
                    };
                    return exportItems<T>(*_buffers, typeCode, encoding, column->size(), getItem);
                } else {
                    throw TuringException("Unsupported type for Optional Vector");
                }
            }
            break;
            case ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T, wasm::WasmSink>) {
                    wasm::ColumnConst<T>* column = static_cast<wasm::ColumnConst<T>*>(_column);
                    const auto getItem = [column](size_t) -> const T* { return &column->getValue(); };
                    return exportItems<T>(*_buffers, typeCode, encoding, column->size(), getItem);
                } else {
                    throw TuringException("Unsupported type for Constant");
                }
            }
            break;
            case ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T, wasm::WasmSink>) {
                    wasm::ColumnConst<std::optional<T>>* column = static_cast<wasm::ColumnConst<std::optional<T>>*>(_column);
                    const auto getItem = [column](size_t) -> const T* {
                        const std::optional<T>& value = column->getValue();
                        return value.has_value() ? &*value : nullptr;
                    };
                    return exportItems<T>(*_buffers, typeCode, encoding, column->size(), getItem);
                } else {
                    throw TuringException("Unsupported type for Optional Constant");
                }
            }
            break;
        }

        throw TuringException("Unsupported column kind");
    }
};

}

// JS-facing decoder: feed it framed Turing Proto packets (as Uint8Array), read the
// decoded columns back as flat buffers.
class TuringDecoder {
public:
    TuringDecoder(uint32_t bufferCapacity)
        : _inBuf(bufferCapacity),
          _decoder(&_inBuf, &_sink, _colSchemas)
    {
    }

    void decodePacket(val packet) {
        const size_t packetSize = packet["length"].as<size_t>();
        if (packetSize < ProtoHeader::wireSize()) {
            throw TuringException("Packet smaller than a proto header");
        }

        std::array<char, ProtoHeader::wireSize()> headerBytes;
        val(emscripten::typed_memory_view(headerBytes.size(), reinterpret_cast<uint8_t*>(headerBytes.data())))
            .call<void>("set", packet.call<val>("subarray", 0u, static_cast<uint32_t>(ProtoHeader::wireSize())));

        const ProtoHeader header = ProtoHeader::decode(headerBytes.data(), headerBytes.size());

        if (packetSize != ProtoHeader::wireSize() + header._dataLen) {
            throw TuringException("Packet size does not match its header dataLen");
        }

        loadPayload(packet, header._dataLen);

        switch (header._type) {
            case MessageTypes::CHUNK_HEADER:
                _decoder.decodeIncomingChunkHeader(&_container);
            break;
            case MessageTypes::CHUNK:
                _decoder.decodeIncomingChunk(&_container);
            break;
            case MessageTypes::END_CHUNK:
            case MessageTypes::END:
            break;
            case MessageTypes::ERROR:
            case MessageTypes::PROTOCOL_ERROR:
                throw TuringException(std::string(_inBuf.data(), _inBuf.size()));
            break;
            default:
                throw TuringException("Unknown packet type");
        }
    }

    uint32_t getColumnCount() const {
        return static_cast<uint32_t>(_container.size());
    }

    std::string getColumnName(uint32_t index) const {
        return _container.getName(index);
    }

    val getColumnBuffers(uint32_t index) {
        const DecodedColumnSchema& schema = _colSchemas.at(index);
        const ColumnWireHeader& header = schema.getHeader();

        return TuringProtoDecoder<wasm::WasmSink>::dispatchColumnType(ColumnInternalKind(header._typeCode),
                                                                      ColumnKind(header._encoding),
                                                                      ColumnBuffersFn {_container[index], &_exportBuffers});
    }

    val getListBytes() {
        const std::span<const char> bytes = _sink.getListBytes();
        return copyToUint8Array(bytes.data(), bytes.size());
    }

    // The strings referenced by index from the list bytes, in the layout of a STRING
    // vector column.
    val getListStrings() {
        const std::span<const std::string_view> strings = _sink.getListStrings();
        const auto getItem = [strings](size_t index) -> const std::string_view* { return &strings[index]; };

        return exportItems<db::types::String::Primitive>(_exportBuffers, ColumnInternalKind::STRING, ColumnKind::VECTOR, strings.size(), getItem);
    }

    // Ends the current dataframe: drops the decoded rows but keeps the columns and
    // their schemas, since the server sends one CHUNK_HEADER per response and streams
    // every dataframe through the same columns. Mirrors the native client's END_CHUNK
    // handling; read the columns out before calling this.
    void endChunk() {
        _container.clearColumnData();

        for (DecodedColumnSchema& schema : _colSchemas) {
            schema.getColumnState().reset();
        }

        _decoder.reset();
    }

    void reset() {
        _decoder.reset();
        _container.clear();
        _colSchemas.clear();
        _inBuf.reset();
    }

private:
    TuringProtoInBuf _inBuf;
    wasm::WasmSink _sink;
    wasm::ColumnContainer _container;
    std::vector<DecodedColumnSchema> _colSchemas;
    TuringProtoDecoder<wasm::WasmSink> _decoder;
    ExportBuffers _exportBuffers;

    // Copies the packet payload straight into the decode buffer through a heap view —
    // one JS set() call, not a per-byte conversion.
    void loadPayload(val packet, size_t dataLen) {
        if (dataLen > _inBuf.capacity()) {
            throw TuringException("Packet payload exceeds decoder buffer capacity");
        }

        _inBuf.reset();

        if (dataLen > 0) {
            val payloadView = val(emscripten::typed_memory_view(dataLen, reinterpret_cast<uint8_t*>(_inBuf.data())));
            payloadView.call<void>("set", packet.call<val>("subarray", static_cast<uint32_t>(ProtoHeader::wireSize())));
            _inBuf.increaseWriteOffset(dataLen);
        }
    }
};

EMSCRIPTEN_BINDINGS(turingdb_decoder) {
    emscripten::class_<TuringDecoder>("TuringDecoder")
        .constructor<uint32_t>()
        .function("decodePacket", &TuringDecoder::decodePacket)
        .function("getColumnCount", &TuringDecoder::getColumnCount)
        .function("getColumnName", &TuringDecoder::getColumnName)
        .function("getColumnBuffers", &TuringDecoder::getColumnBuffers)
        .function("getListBytes", &TuringDecoder::getListBytes)
        .function("getListStrings", &TuringDecoder::getListStrings)
        .function("endChunk", &TuringDecoder::endChunk)
        .function("reset", &TuringDecoder::reset);
}
