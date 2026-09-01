#include "TuringProtoDecoder.h"

#include <string>
#include <algorithm>

#include "Bitmask.h"
#include "ChunkedBuffer.h"
#include "TuringProtoHeaders.h"
#include "Decoders.h"
#include "TuringProtoInBuf.h"
#include "TuringProtoOutBuf.h"
#include "GraphPath.h"
#include "TuringException.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "ID.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringSink.h"
#include "TuringSinkColumnContainer.h"

using namespace net::proto;

namespace {

template <typename Fn>
decltype(auto) dispatchColumnType(net::proto::ColumnInternalKind typeCode, net::proto::ColumnKind encoding, Fn&& fn) {
    switch (typeCode) {
        case net::proto::ColumnInternalKind::UINT64:
            return fn.template operator()<db::types::UInt64::Primitive>(encoding);
        case net::proto::ColumnInternalKind::INT64:
            return fn.template operator()<db::types::Int64::Primitive>(encoding);
        case net::proto::ColumnInternalKind::DOUBLE:
            return fn.template operator()<db::types::Double::Primitive>(encoding);
        case net::proto::ColumnInternalKind::STRING:
            return fn.template operator()<std::string>(encoding);
        case net::proto::ColumnInternalKind::BOOL:
            return fn.template operator()<db::types::Bool::Primitive>(encoding);
        case net::proto::ColumnInternalKind::PATH:
            return fn.template operator()<db::Path>(encoding);
        case net::proto::ColumnInternalKind::EMBEDDING:
            return fn.template operator()<db::types::Embedding::Primitive>(encoding);
        case net::proto::ColumnInternalKind::VALUE_TYPE:
            return fn.template operator()<db::ValueType>(encoding);
        case net::proto::ColumnInternalKind::ENTITY_LIST:
            return fn.template operator()<db::EntityList>(encoding);
        case net::proto::ColumnInternalKind::LIST_VIEW:
            return fn.template operator()<db::ListView>(encoding);
        case net::proto::ColumnInternalKind::LIST_ELEMENT_VIEW:
            return fn.template operator()<db::ListElementView>(encoding);
        case net::proto::ColumnInternalKind::NODE_ID:
            return fn.template operator()<db::NodeID>(encoding);
        case net::proto::ColumnInternalKind::EDGE_ID:
            return fn.template operator()<db::EdgeID>(encoding);
        case net::proto::ColumnInternalKind::EDGE_TYPE_ID:
            return fn.template operator()<db::EdgeTypeID>(encoding);
        case net::proto::ColumnInternalKind::PROPERTY_TYPE_ID:
            return fn.template operator()<db::PropertyTypeID>(encoding);
        case net::proto::ColumnInternalKind::LABEL_ID:
            return fn.template operator()<db::LabelID>(encoding);
        case net::proto::ColumnInternalKind::LABEL_SET_ID:
            return fn.template operator()<db::LabelSetID>(encoding);
        case net::proto::ColumnInternalKind::CHANGE_ID:
            return fn.template operator()<db::ChangeID>(encoding);
        case net::proto::ColumnInternalKind::PROPERTY_NULL:
            return fn.template operator()<db::PropertyNull>(encoding);
        default:
            break;
    }

    throw TuringException("Unsupported incoming column type");
}

template<typename Sink>
struct MakeColumnFn {
    Sink* _sink {nullptr};

    template <typename T>
    db::Column* operator()(net::proto::ColumnKind encoding) const {
        switch (encoding) {
            case net::proto::ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T>) {
                    return _sink->template alloc<typename Sink::template ColumnVector<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Vector");
                }
            }
            break;
            case net::proto::ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T>) {
                    return _sink->template alloc<typename Sink::template ColumnConst<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Constant");
                }
            }
            break;
            case net::proto::ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T>) {
                    return _sink->template alloc<typename Sink::template ColumnOptVector<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Vector");
                }
            }
            break;
            case net::proto::ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T>) {
                    return _sink->template alloc<typename Sink::template ColumnOptConst<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Const");
                }
            }
            break;
            default: {
                throw TuringException("Unsupported incoming column kind");
            }
        }
    }
};

template<typename Sink>
struct DecodeColumnFn {
    TuringProtoDecoder<Sink>* _decoder {nullptr};
    db::Column* _column {nullptr};
    DfColumnState* _columnState {nullptr};

    template <typename T>
    bool operator()(net::proto::ColumnKind encoding) {
        switch (encoding) {
            case net::proto::ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T>) {
                    auto* typedCol = static_cast<typename Sink::template ColumnVector<T>*>(_column);
                    return _decoder->decodeColumn(typedCol, _columnState);
                } else {
                    throw TuringException("Unsupported type for Vector");
                }
            }
            case net::proto::ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T>) {
                    auto* typedCol = static_cast<typename Sink::template ColumnOptVector<T>*>(_column);
                    return _decoder->decodeColumn(typedCol, _columnState);
                } else {
                    throw TuringException("Unsupported type for Optional Vector");
                }
            }
            case net::proto::ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T>) {
                    auto* typedCol = static_cast<typename Sink::template ColumnConst<T>*>(_column);
                    return _decoder->decodeColumn(typedCol);
                } else {
                    throw TuringException("Unsupported type for Constant");
                }
            }
            case net::proto::ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T>) {
                    auto* typedCol = static_cast<typename Sink::template ColumnConst<std::optional<T>>*>(_column);
                    return _decoder->decodeColumn(typedCol);
                } else {
                    throw TuringException("Unsupported type for Optional Constant");
                }
            }
            default:
                throw TuringException("Unsupported column kind");
        }
    }
};

}

template <typename Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(typename Sink::template ColumnVector<T>* col, DfColumnState* colState) {
    if (colState->getNumRows() == 0) {
        if (_ctxt._inBuf->readable() < sizeof(WireSize)) {
            return false;
        }
        WireSize numRows = 0;
        _ctxt._inBuf->readData(&numRows, sizeof(numRows));
        colState->setNumRows(numRows);
    }

    return decodeVector<T>(&_ctxt, _sink, col, colState);
}

template <typename Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(typename Sink::template ColumnVector<std::optional<T>>* col, DfColumnState* colState) {
    //Check if this is the first row in the column we are decoding
    if (colState->getNumRows() == 0) {
        //if the wiresize length is not available in the input buffer continue later
        if (_ctxt._inBuf->readable() < sizeof(WireSize)) {
            return false;
        }
        WireSize numRows = 0;
        _ctxt._inBuf->readData(&numRows, sizeof(numRows));
        colState->setNumRows(numRows);
    }

    //Checking if we haven't read any rows of the column, to see if we should resize the
    //column
    if (_ctxt._rowIndex == 0) {
        col->resize(colState->getNumRows());
    }

    //Check if we have read the bit mask size yet so we can resize our bit mask
    if (colState->getBitMask().size() == 0) {
        colState->getBitMask().resize(colState->getNumRows());

        const size_t bytesToCopy = std::min(_ctxt._inBuf->readable(), colState->getBitMask().byteSize());
        _ctxt._inBuf->readData(colState->getBitMask().data(), bytesToCopy);

        //If the full bitmask is not available in the buffer (likely split across packets)
        // we then save the appropriate data to the buffer state so we can coppy directly
        // to the bitmask buffer as soon as the new packet comes in.
        if (bytesToCopy != colState->getBitMask().byteSize()) {
            _ctxt._bufferState._start = reinterpret_cast<char*>(colState->getBitMask().data());
            _ctxt._bufferState._len = colState->getBitMask().byteSize();
            _ctxt._bufferState._offset = bytesToCopy;
            return false;
        }
    }

    return decodeOptVector<T>(&_ctxt, _sink, col, colState);
}

template <typename Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(typename Sink::template ColumnConst<T>* col) {
    return decodeConst<T>(&_ctxt, _sink, col);
}

template <typename Sink>
template <typename T>
bool TuringProtoDecoder<Sink>::decodeColumn(typename Sink::template ColumnConst<std::optional<T>>* col) {
    return decodeOptConst<T>(&_ctxt, _sink, col);
}

template <typename Sink>
TuringProtoDecoder<Sink>::TuringProtoDecoder(TuringProtoInBuf* inBuf,
                                             Sink* sink,
                                             std::vector<DecodedColumnSchema>& colSchemas)
    : _colSchemas(colSchemas),
    _sink(sink)
{
    _ctxt._inBuf = inBuf;
}

template <typename Sink>
void TuringProtoDecoder<Sink>::reset() {
    _ctxt.reset();
    _sink->reset();
}

template<typename Sink>
void TuringProtoDecoder<Sink>::decodeIncomingData(Sink::ColumnContainer* container) {
    bioassert(container->size() == _colSchemas.size(), "Column container should have the same number of columns as we have read from the packet");

    while (_ctxt._colIndex < container->size()) {
        auto& schema = _colSchemas[_ctxt._colIndex];
        auto* col = (*container)[_ctxt._colIndex];

        const bool completed = dispatchColumnType(
            net::proto::ColumnInternalKind(schema.getHeader()._typeCode),
            net::proto::ColumnKind(schema.getHeader()._encoding),
            DecodeColumnFn {this, col, &schema.getColState()});

        if (!completed) {
            break;
        }

        ++_ctxt._colIndex;
        _ctxt._rowIndex = 0;
    }
}

template<typename Sink>
void TuringProtoDecoder<Sink>::decodeIncomingChunkHeader(Sink::ColumnContainer* container) {
    bioassert(container, "decodeIncomingChunkHeader called with null column container");

    WireSize columnCount = 0;
    _ctxt._inBuf->readData(&columnCount, sizeof(columnCount));

    _colSchemas.resize(columnCount);

    for (WireSize i = 0; i < columnCount; ++i) {
        auto& schema = _colSchemas[i];
        _ctxt._inBuf->readHeader(&(schema.getHeader()));

        _ctxt._inBuf->ensureReadable(schema.getHeader()._nameLen);
        schema.setColName(std::string(_ctxt._inBuf->readPtr(), schema.getHeader()._nameLen));
        _ctxt._inBuf->increaseReadOffset(schema.getHeader()._nameLen);

        auto* column = dispatchColumnType(
            net::proto::ColumnInternalKind(schema.getHeader()._typeCode),
            net::proto::ColumnKind(schema.getHeader()._encoding),
            MakeColumnFn<Sink> {_sink});

        container->addColumn(column, schema.getColName());
    }
}

template<typename Sink>
void TuringProtoDecoder<Sink>::decodeIncomingChunk(Sink::ColumnContainer* container) {
    bioassert(container, "decodeIncomingChunk called with null column container");

    if (_ctxt._bufferState._start != nullptr) {
        const size_t dataLeftToRead = _ctxt._bufferState._len - _ctxt._bufferState._offset;
        const size_t lenToCopy = std::min(dataLeftToRead, _ctxt._inBuf->readable());

        _ctxt._inBuf->readData(_ctxt._bufferState._start + _ctxt._bufferState._offset, lenToCopy);
        _ctxt._bufferState._offset += lenToCopy;

        if (_ctxt._bufferState._offset == _ctxt._bufferState._len) {
            _ctxt._bufferState._start = nullptr;
        }
    }

    if (_ctxt._inBuf->readable() == 0) {
        return;
    }

    decodeIncomingData(container);
}

// The decoder's member definitions live in this translation unit; every sink the
// native build supports must be explicitly instantiated here.
template class net::proto::TuringProtoDecoder<TuringSink>;
