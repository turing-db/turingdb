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
#include "dataframe/Dataframe.h"
#include "dataframe/DataframeManager.h"
#include "dataframe/NamedColumn.h"
#include "ID.h"
#include "LocalMemory.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "TuringProtoDecoderConcepts.h"

#include "FatalException.h"

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
            return fn.template operator()<db::types::String::Primitive>(encoding);
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

struct MakeColumnFn {
    db::LocalMemory* _localMem {nullptr};

    template <typename T>
    db::Column* operator()(net::proto::ColumnKind encoding) const {
        switch (encoding) {
            case net::proto::ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T>) {
                    return _localMem->alloc<db::ColumnVector<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Vector");
                }
            }
            break;

            case net::proto::ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T>) {
                    return _localMem->alloc<db::ColumnConst<T>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Constant");
                }
            }
            break;

            case net::proto::ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T>) {
                    return _localMem->alloc<db::ColumnVector<std::optional<T>>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Vector");
                }
            }
            break;

            case net::proto::ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T>) {
                    return _localMem->alloc<db::ColumnConst<std::optional<T>>>();
                } else {
                    throw TuringException("Unsupported internal kind for type:Optional Const");
                }
            }
            break;
        }

        throw FatalException("Unknown proto::ColumnKind.");
    }
};

struct DecodeColumnFn {
    TuringProtoDecoder* _decoder {nullptr};
    db::Column* _column {nullptr};
    DfColumnState* _columnState {nullptr};

    template <typename T>
    bool operator()(net::proto::ColumnKind encoding) {
        switch (encoding) {
            case net::proto::ColumnKind::VECTOR: {
                if constexpr (SupportedColumnVectorTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnVector<T>*>(_column);
                    return _decoder->decodeColumn(typedCol, _columnState);
                } else {
                    throw TuringException("Unsupported type for Vector");
                }
            }
            break;

            case net::proto::ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnVector<std::optional<T>>*>(_column);
                    return _decoder->decodeColumn(typedCol, _columnState);
                } else {
                    throw TuringException("Unsupported type for Optional Vector");
                }
            } break;

            case net::proto::ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnConst<T>*>(_column);
                    return _decoder->decodeColumn(typedCol);
                } else {
                    throw TuringException("Unsupported type for Constant");
                }
            } break;

            case net::proto::ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnConst<std::optional<T>>*>(_column);
                    return _decoder->decodeColumn(typedCol);
                } else {
                    throw TuringException("Unsupported type for Optional Constant");
                }
            }
            break;
        }

        throw FatalException("Invalid proto::ColumnKind.");
    }
};

}

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnVector<T>* col, DfColumnState* colState) {
    if (colState->getNumRows() == 0) {
        if (_ctxt._inBuf->readable() < sizeof(WireSize)) {
            return false;
        }
        WireSize numRows = 0;
        _ctxt._inBuf->readData(&numRows, sizeof(numRows));
        colState->setNumRows(numRows);
    }

    return decodeVector(&_ctxt, col, colState);
}

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnVector<std::optional<T>>* col, DfColumnState* colState) {
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

    return decodeOptVector(&_ctxt, col, colState);
}

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnConst<T>* col) {
    return decodeConst(&_ctxt, col);
}

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnConst<std::optional<T>>* col) {
    return decodeOptConst(&_ctxt, col);
}

TuringProtoDecoder::TuringProtoDecoder(db::LocalMemory* localMem,
                                       db::DataframeManager* dfMan,
                                       TuringProtoInBuf* inBuf,
                                       ChunkedBuffer<float>* embeddingBuffer,
                                       ChunkedBuffer<char>* stringBuffer,
                                       db::ListBuffer<>* listBuffer,
                                       std::vector<DecodedColumnSchema>& colSchemas)
    : _localMem(localMem),
    _dfMan(dfMan),
    _colSchemas(colSchemas)
{
    _ctxt._inBuf = inBuf;
    _ctxt._embeddingBuffer = embeddingBuffer;
    _ctxt._stringBuffer = stringBuffer;
    _ctxt._listBuffer = listBuffer;
    _ctxt._mem = _localMem;
}

void TuringProtoDecoder::reset() {
    _ctxt.reset();
}

void TuringProtoDecoder::decodeIncomingData(db::Dataframe* df) {
    bioassert(df->size() == _colSchemas.size(), "Dataframe should have the same number of columns as we have read from the packet");

    while (_ctxt._colIndex < df->size()) {
        auto& schema = _colSchemas[_ctxt._colIndex];
        auto* col = df->cols()[_ctxt._colIndex]->getColumn();

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

void TuringProtoDecoder::decodeIncomingChunkHeader(db::Dataframe* df) {
    bioassert(df, "decodeIncomingChunkHeader called with null dataframe");

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
            MakeColumnFn {_localMem});

        db::NamedColumn* namedColumn = db::NamedColumn::create(_dfMan, column, _dfMan->allocTag());

        namedColumn->rename(schema.getColName());
        df->addColumn(namedColumn);
    }
}

void TuringProtoDecoder::decodeIncomingChunk(db::Dataframe* df) {
    bioassert(df, "decodeIncomingChunk called with null dataframe");

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

    decodeIncomingData(df);
}
