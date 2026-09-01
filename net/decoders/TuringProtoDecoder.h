#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <optional>
#include <string>
#include <vector>

#include "DecodeContext.h"
#include "DecodedColumnSchema.h"
#include "Decoders.h"
#include "ProtoDecodeSink.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringProtoHeaders.h"
#include "TuringProtoInBuf.h"
#include "TuringException.h"

#include "BioAssert.h"

namespace net::proto {

// A templated class containing the protocol decoding logic. It expects to be templated
// with a sink class that provides the underlying storage to write the decoded bytes into
// (the sink)
template <ProtoDecodeSink Sink>
class TuringProtoDecoder {
public:
    TuringProtoDecoder(TuringProtoInBuf* inBuf,
                       Sink* sink,
                       std::vector<DecodedColumnSchema>& columnSchemas);

    // Decode a received column header chunk - this should be called once per header
    // at the start of a response.
    void decodeIncomingChunkHeader(SinkColumnContainer<Sink>* container);

    // Decode an incoming data chunk - we first check if we have some unfinished state
    // from the previous chunk and fill that up first
    void decodeIncomingChunk(SinkColumnContainer<Sink>* container);
    // Decode new data from the incoming chunk
    void decodeIncomingData(SinkColumnContainer<Sink>* container);

    void reset();

    template <typename T>
    bool decodeColumn(SinkColumnVector<T, Sink>* column, ProtoColumnState* columnState);

    template <typename T>
    bool decodeColumn(SinkColumnOptVector<T, Sink>* column, ProtoColumnState* columnState);

    template <typename T>
    bool decodeColumn(SinkColumnConst<T, Sink>* column);

    template <typename T>
    bool decodeColumn(SinkColumnOptConst<T, Sink>* column);

    // Public so family-side code (e.g. a JS accessor converting decoded columns) can
    // reuse the wire-type dispatch.
    template <typename Fn>
    static decltype(auto) dispatchColumnType(ColumnInternalKind typeCode, ColumnKind encoding, Fn&& fn);

private:
    struct MakeColumnFn;
    struct DecodeColumnFn;

    std::vector<DecodedColumnSchema>& _columnSchemas;
    DecodeContext _context;
    Sink* _sink {nullptr};
};

// Maps a column's wire type code to the concrete element type and invokes @param fn with it.
template <ProtoDecodeSink Sink>
template <typename Fn>
decltype(auto) TuringProtoDecoder<Sink>::dispatchColumnType(ColumnInternalKind typeCode, ColumnKind encoding, Fn&& fn) {
    switch (typeCode) {
        case ColumnInternalKind::UINT64:
            return fn.template operator()<db::types::UInt64::Primitive>(encoding);
        case ColumnInternalKind::INT64:
            return fn.template operator()<db::types::Int64::Primitive>(encoding);
        case ColumnInternalKind::DOUBLE:
            return fn.template operator()<db::types::Double::Primitive>(encoding);
        case ColumnInternalKind::STRING:
            return fn.template operator()<db::types::String::Primitive>(encoding);
        case ColumnInternalKind::BOOL:
            return fn.template operator()<db::types::Bool::Primitive>(encoding);
        case ColumnInternalKind::PATH:
            return fn.template operator()<db::Path>(encoding);
        case ColumnInternalKind::EMBEDDING:
            return fn.template operator()<db::types::Embedding::Primitive>(encoding);
        case ColumnInternalKind::VALUE_TYPE:
            return fn.template operator()<db::ValueType>(encoding);
        case ColumnInternalKind::ENTITY_LIST:
            return fn.template operator()<db::EntityList>(encoding);
        case ColumnInternalKind::LIST_VIEW:
            return fn.template operator()<SinkListView<Sink>>(encoding);
        case ColumnInternalKind::LIST_ELEMENT_VIEW:
            return fn.template operator()<SinkListElementView<Sink>>(encoding);
        case ColumnInternalKind::NODE_ID:
            return fn.template operator()<db::NodeID>(encoding);
        case ColumnInternalKind::EDGE_ID:
            return fn.template operator()<db::EdgeID>(encoding);
        case ColumnInternalKind::EDGE_TYPE_ID:
            return fn.template operator()<db::EdgeTypeID>(encoding);
        case ColumnInternalKind::PROPERTY_TYPE_ID:
            return fn.template operator()<db::PropertyTypeID>(encoding);
        case ColumnInternalKind::LABEL_ID:
            return fn.template operator()<db::LabelID>(encoding);
        case ColumnInternalKind::LABEL_SET_ID:
            return fn.template operator()<db::LabelSetID>(encoding);
        case ColumnInternalKind::CHANGE_ID:
            return fn.template operator()<db::ChangeID>(encoding);
        case ColumnInternalKind::COMMIT_HASH:
            return fn.template operator()<db::CommitHash>(encoding);
        case ColumnInternalKind::PROPERTY_NULL:
            return fn.template operator()<db::PropertyNull>(encoding);
    }

    throw TuringException("Unsupported incoming column type");
}

// Allocates the sink's column for a decoded column header's (type, encoding) pair.
template <ProtoDecodeSink Sink>
struct TuringProtoDecoder<Sink>::MakeColumnFn {
    Sink* _sink {nullptr};

    template <typename T>
    SinkColumn<Sink>* operator()(ColumnKind encoding) const {
        switch (encoding) {
            case ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T, Sink>) {
                    return _sink->template alloc<SinkColumnVector<T, Sink>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Vector");
                }
            }
            break;
            case ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T, Sink>) {
                    return _sink->template alloc<SinkColumnConst<T, Sink>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Constant");
                }
            }
            break;
            case ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T, Sink>) {
                    return _sink->template alloc<SinkColumnOptVector<T, Sink>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Vector");
                }
            }
            break;
            case ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T, Sink>) {
                    return _sink->template alloc<SinkColumnOptConst<T, Sink>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Const");
                }
            }
        }
        throw TuringException("Unsupported incoming column kind");
    }
};

// Casts a decoded column to the sink's concrete column for its (type, encoding) pair and
// runs the matching decode path.
template <ProtoDecodeSink Sink>
struct TuringProtoDecoder<Sink>::DecodeColumnFn {
    TuringProtoDecoder<Sink>* _decoder {nullptr};
    SinkColumn<Sink>* _column {nullptr};
    ProtoColumnState* _columnState {nullptr};

    template <typename T>
    bool operator()(ColumnKind encoding) {
        switch (encoding) {
            case ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T, Sink>) {
                    auto* typedColumn = static_cast<SinkColumnVector<T, Sink>*>(_column);
                    return _decoder->decodeColumn(typedColumn, _columnState);
                } else {
                    throw TuringException("Unsupported type for Vector");
                }
            }
            case ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T, Sink>) {
                    auto* typedColumn = static_cast<SinkColumnOptVector<T, Sink>*>(_column);
                    return _decoder->decodeColumn(typedColumn, _columnState);
                } else {
                    throw TuringException("Unsupported type for Optional Vector");
                }
            }
            case ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T, Sink>) {
                    auto* typedColumn = static_cast<SinkColumnConst<T, Sink>*>(_column);
                    return _decoder->decodeColumn(typedColumn);
                } else {
                    throw TuringException("Unsupported type for Constant");
                }
            }
            case ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T, Sink>) {
                    auto* typedColumn = static_cast<SinkColumnOptConst<T, Sink>*>(_column);
                    return _decoder->decodeColumn(typedColumn);
                } else {
                    throw TuringException("Unsupported type for Optional Constant");
                }
            }
        }
        throw TuringException("Unsupported column kind");
    }
};

template <ProtoDecodeSink Sink>
TuringProtoDecoder<Sink>::TuringProtoDecoder(TuringProtoInBuf* inBuf,
                                             Sink* sink,
                                             std::vector<DecodedColumnSchema>& columnSchemas)
    : _columnSchemas(columnSchemas),
    _sink(sink)
{
    _context._inBuf = inBuf;
}

template <ProtoDecodeSink Sink>
void TuringProtoDecoder<Sink>::reset() {
    _context.reset();
    _sink->reset();
}

template <ProtoDecodeSink Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(SinkColumnVector<T, Sink>* column, ProtoColumnState* columnState) {
    if (columnState->getNumRows() == 0) {
        if (_context._inBuf->readable() < sizeof(WireSize)) {
            return false;
        }
        WireSize numRows = 0;
        _context._inBuf->readData(&numRows, sizeof(numRows));
        columnState->setNumRows(numRows);
    }

    return VectorColumnDecoder<T, Sink>::decode(&_context, _sink, column, columnState);
}

template <ProtoDecodeSink Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(SinkColumnOptVector<T, Sink>* column, ProtoColumnState* columnState) {
    // Check if this is the first row in the column we are decoding
    if (columnState->getNumRows() == 0) {
        // if the wiresize length is not available in the input buffer continue later
        if (_context._inBuf->readable() < sizeof(WireSize)) {
            return false;
        }
        WireSize numRows = 0;
        _context._inBuf->readData(&numRows, sizeof(numRows));
        columnState->setNumRows(numRows);
    }

    // Checking if we haven't read any rows of the column, to see if we should resize the
    // column
    if (_context._rowIndex == 0) {
        column->resize(columnState->getNumRows());
    }

    // Check if we have read the bit mask size yet so we can resize our bit mask
    if (columnState->getBitMask().size() == 0) {
        columnState->getBitMask().resize(columnState->getNumRows());

        const size_t bytesToCopy = std::min(_context._inBuf->readable(), columnState->getBitMask().byteSize());
        _context._inBuf->readData(columnState->getBitMask().data(), bytesToCopy);

        // If the full bitmask is not available in the buffer (likely split across packets)
        //  we then save the appropriate data to the buffer state so we can copy directly
        //  to the bitmask buffer as soon as the new packet comes in.
        if (bytesToCopy != columnState->getBitMask().byteSize()) {
            _context._bufferState._start = reinterpret_cast<char*>(columnState->getBitMask().data());
            _context._bufferState._len = columnState->getBitMask().byteSize();
            _context._bufferState._offset = bytesToCopy;
            return false;
        }
    }

    return OptionalVectorColumnDecoder<T, Sink>::decode(&_context, _sink, column, columnState);
}

template <ProtoDecodeSink Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(SinkColumnConst<T, Sink>* column) {
    return ConstColumnDecoder<T, Sink>::decode(&_context, _sink, column);
}

template <ProtoDecodeSink Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(SinkColumnOptConst<T, Sink>* column) {
    // Const columns have no row dimension, so reuse _rowIndex as a 0/1 "hasValue flag
    // read and true" marker.
    if (_context._rowIndex == 0) {
        if (_context._inBuf->readable() < sizeof(uint8_t)) {
            return false;
        }

        uint8_t hasValue = 0;
        _context._inBuf->readData(&hasValue, sizeof(hasValue));

        if (hasValue == 0) {
            column->set(std::nullopt);
            return true;
        }
        _context._rowIndex = 1;
    }

    return ConstColumnDecoder<T, Sink>::decode(&_context, _sink, column);
}

template <ProtoDecodeSink Sink>
void TuringProtoDecoder<Sink>::decodeIncomingData(SinkColumnContainer<Sink>* container) {
    bioassert(container->size() == _columnSchemas.size(), "Column container should have the same number of columns as we have read from the packet");

    while (_context._columnIndex < container->size()) {
        auto& schema = _columnSchemas[_context._columnIndex];
        auto* column = (*container)[_context._columnIndex];

        const bool completed = dispatchColumnType(
            ColumnInternalKind(schema.getHeader()._typeCode),
            ColumnKind(schema.getHeader()._encoding),
            DecodeColumnFn {this, column, &schema.getColumnState()});

        if (!completed) {
            break;
        }

        ++_context._columnIndex;
        _context._rowIndex = 0;
    }
}

template <ProtoDecodeSink Sink>
void TuringProtoDecoder<Sink>::decodeIncomingChunkHeader(SinkColumnContainer<Sink>* container) {
    bioassert(container, "decodeIncomingChunkHeader called with null column container");

    WireSize columnCount = 0;
    _context._inBuf->readData(&columnCount, sizeof(columnCount));

    _columnSchemas.resize(columnCount);

    for (WireSize i = 0; i < columnCount; ++i) {
        auto& schema = _columnSchemas[i];
        _context._inBuf->readHeader(&(schema.getHeader()));

        _context._inBuf->ensureReadable(schema.getHeader()._nameLen);
        schema.setColumnName(std::string(_context._inBuf->readPtr(), schema.getHeader()._nameLen));
        _context._inBuf->increaseReadOffset(schema.getHeader()._nameLen);

        auto* column = dispatchColumnType(
            ColumnInternalKind(schema.getHeader()._typeCode),
            ColumnKind(schema.getHeader()._encoding),
            MakeColumnFn {_sink});

        container->addColumn(column, schema.getColumnName());
    }
}

template <ProtoDecodeSink Sink>
void TuringProtoDecoder<Sink>::decodeIncomingChunk(SinkColumnContainer<Sink>* container) {
    bioassert(container, "decodeIncomingChunk called with null column container");

    // drain bytes into any unfinished buffered state first
    if (_context._bufferState._start != nullptr) {
        const size_t dataLeftToRead = _context._bufferState._len - _context._bufferState._offset;
        const size_t lenToCopy = std::min(dataLeftToRead, _context._inBuf->readable());

        _context._inBuf->readData(_context._bufferState._start + _context._bufferState._offset, lenToCopy);
        _context._bufferState._offset += lenToCopy;

        if (_context._bufferState._offset == _context._bufferState._len) {
            _context._bufferState._start = nullptr;
        }
    }

    if (_context._inBuf->readable() == 0) {
        return;
    }

    decodeIncomingData(container);
}

}
