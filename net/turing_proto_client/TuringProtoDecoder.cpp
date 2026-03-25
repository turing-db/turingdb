#include "TuringProtoDecoder.h"

#include <string>
#include <algorithm>

#include "Bitmask.h"
#include "EmbeddingBuffer.h"
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
            default: {
                throw TuringException("Unsupported incoming column kind");
            }
        }
    }
};

struct DecodeColumnFn {
    TuringProtoDecoder* _decoder {nullptr};
    db::Column* _column {nullptr};
    TuringProtoDecoder::DfColumnState* _columnState {nullptr};

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
            case net::proto::ColumnKind::OPTIONAL_VECTOR: {
                if constexpr (SupportedColumnOptVectorTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnVector<std::optional<T>>*>(_column);
                    return _decoder->decodeColumn(typedCol, _columnState);
                } else {
                    throw TuringException("Unsupported type for Optional Vector");
                }
            }
            case net::proto::ColumnKind::CONSTANT: {
                if constexpr (SupportedColumnConstTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnConst<T>*>(_column);
                    return _decoder->decodeColumn(typedCol);
                } else {
                    throw TuringException("Unsupported type for Constant");
                }
            }
            case net::proto::ColumnKind::OPTIONAL_CONSTANT: {
                if constexpr (SupportedColumnOptConstTypes<T>) {
                    auto* typedCol = static_cast<db::ColumnConst<std::optional<T>>*>(_column);
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

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnVector<T>* col, DfColumnState* colState) {
    if (colState->_numRows == 0) {
        if (_ctxt._inBuf->readable() < sizeof(colState->_numRows)) {
            return false;
        }
        _ctxt._inBuf->readData(&colState->_numRows, sizeof(colState->_numRows));
    }

    return decodeVector(&_ctxt, col, colState);
}

template <typename T>
bool TuringProtoDecoder::decodeColumn(db::ColumnVector<std::optional<T>>* col, DfColumnState* colState) {
    if (colState->_numRows == 0) {
        if (_ctxt._inBuf->readable() < sizeof(colState->_numRows)) {
            return false;
        }
        _ctxt._inBuf->readData(&colState->_numRows, sizeof(colState->_numRows));
    }

    if (_ctxt._rowIndex == 0) {
        col->resize(colState->_numRows);
    }

    if (colState->_bitMask.size() == 0) {
        colState->_bitMask.resize(colState->_numRows);

        const size_t bytesToCopy = std::min(_ctxt._inBuf->readable(), colState->_bitMask.byteSize());
        _ctxt._inBuf->readData(colState->_bitMask.data(), bytesToCopy);

        if (bytesToCopy != colState->_bitMask.byteSize()) {
            _ctxt._bufferState._start = reinterpret_cast<char*>(colState->_bitMask.data());
            _ctxt._bufferState._len = colState->_bitMask.byteSize();
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
                                       EmbeddingBuffer* embeddingBuffer)
    : _localMem(localMem),
    _dfMan(dfMan)
{
    _ctxt._inBuf = inBuf;
    _ctxt._embeddingBuffer = embeddingBuffer;
}

void TuringProtoDecoder::reset() {
    _ctxt.reset();
}

void TuringProtoDecoder::decodeIncomingData(db::Dataframe* df,
                                            std::vector<DecodedColumnSchema>& colSchemas) {
    bioassert(df->size() == colSchemas.size(), "Dataframe should have the same number of columns as we have read from the packet");

    while (_ctxt._colIndex < df->size()) {
        auto& schema = colSchemas[_ctxt._colIndex];
        auto* col = df->cols()[_ctxt._colIndex]->getColumn();

        const bool completed = dispatchColumnType(
            net::proto::ColumnInternalKind(schema._header._typeCode),
            net::proto::ColumnKind(schema._header._encoding),
            DecodeColumnFn {this, col, &schema._colState});

        if (!completed) {
            break;
        }

        ++_ctxt._colIndex;
        _ctxt._rowIndex = 0;
    }
}

void TuringProtoDecoder::decodeIncomingChunkHeader(db::Dataframe* df,
                                                   std::vector<DecodedColumnSchema>& colSchemas) {
    bioassert(df, "decodeIncomingChunkHeader called with null dataframe");

    uint32_t columnCount = 0;
    _ctxt._inBuf->readData(&columnCount, sizeof(columnCount));

    colSchemas.resize(columnCount);

    for (uint32_t i = 0; i < columnCount; ++i) {
        auto& schema = colSchemas[i];
        _ctxt._inBuf->readHeader(&(schema._header));

        _ctxt._inBuf->ensureReadable(schema._header._nameLen);
        schema._colName = std::string(_ctxt._inBuf->readPtr(), schema._header._nameLen);
        _ctxt._inBuf->increaseReadOffset(schema._header._nameLen);

        auto* column = dispatchColumnType(
            net::proto::ColumnInternalKind(schema._header._typeCode),
            net::proto::ColumnKind(schema._header._encoding),
            MakeColumnFn {_localMem});

        db::NamedColumn* namedColumn = db::NamedColumn::create(_dfMan, column, _dfMan->allocTag());

        namedColumn->rename(schema._colName);
        df->addColumn(namedColumn);
    }
}

void TuringProtoDecoder::decodeIncomingChunk(db::Dataframe* df,
                                             std::vector<DecodedColumnSchema>& colSchemas) {
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

    decodeIncomingData(df, colSchemas);
}
