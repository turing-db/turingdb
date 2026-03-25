#pragma once

#include <stddef.h>
#include <span>
#include <string>

#include "EmbeddingBuffer.h"
#include "GraphPath.h"
#include "TuringProtoDecoder.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringProtoInBuf.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"

namespace net::proto {

template <typename T>
inline bool decodeVector(TuringProtoDecoder::DecodeContext* ctx,
                         db::ColumnVector<T>* typedCol,
                         TuringProtoDecoder::DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeVector");

    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->_numRows);
    }

    const size_t rowsRemaining = columnState->_numRows - ctx->_rowIndex;

    if (rowsRemaining * sizeof(T) > ctx->_inBuf->readable()) {
        const size_t numRowsRead = ctx->_inBuf->readable() / sizeof(T);
        ctx->_inBuf->readData(typedCol->data() + ctx->_rowIndex, numRowsRead * sizeof(T));
        ctx->_rowIndex += numRowsRead;
        return false;
    }

    ctx->_inBuf->readData(typedCol->data() + ctx->_rowIndex, rowsRemaining * sizeof(T));
    return true;
}

template <>
inline bool decodeVector<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                      db::ColumnVector<std::string>* typedCol,
                                      TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t stringSize = 0;
        ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

        if (stringSize <= ctx->_inBuf->readable()) {
            typedCol->emplace_back(ctx->_inBuf->readPtr(), stringSize);
            ctx->_inBuf->increaseReadOffset(stringSize);
        } else {
            auto& val = typedCol->emplace_back(stringSize, '\0');
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(val.data(), numBytesToRead);

            ctx->_bufferState._start = val.data();
            ctx->_bufferState._len = val.size();
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::Path>(TuringProtoDecoder::DecodeContext* ctx,
                                   db::ColumnVector<db::Path>* typedCol,
                                   TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t pathByteSize = 0;
        ctx->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
        const size_t numEntities = pathByteSize / sizeof(db::EntityID);

        if (pathByteSize <= ctx->_inBuf->readable()) {
            auto& path = typedCol->emplace_back(numEntities);
            ctx->_inBuf->readData(path.data(), pathByteSize);
        } else {
            auto& path = typedCol->emplace_back(numEntities);
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(path.data(), numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(path.data());
            ctx->_bufferState._len = pathByteSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                          db::ColumnVector<db::types::Embedding::Primitive>* typedCol,
                                                          TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->reserve(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        uint32_t embeddingSize = 0;
        ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
        const size_t numFloats = embeddingSize / sizeof(float);

        auto* data = ctx->_embeddingBuffer->alloc(numFloats);
        const std::span<const float> span = ctx->_embeddingBuffer->getSpan(data, numFloats);

        if (embeddingSize <= ctx->_inBuf->readable()) {
            ctx->_inBuf->readData(data, embeddingSize);
            typedCol->emplace_back(span);
        } else {
            typedCol->emplace_back(span);

            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(data, numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(data);
            ctx->_bufferState._len = embeddingSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeVector<db::EntityList>(TuringProtoDecoder::DecodeContext* ctx,
                                         db::ColumnVector<db::EntityList>* typedCol,
                                         TuringProtoDecoder::DfColumnState* columnState) {
    if (ctx->_rowIndex == 0) {
        typedCol->resize(columnState->_numRows);
    }

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        auto& entityList = (*typedCol)[ctx->_rowIndex + i];

        if (entityList.size() == 0) {
            if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
                ctx->_rowIndex += i;
                return false;
            }

            uint32_t numberOfEntries = 0;
            ctx->_inBuf->readData(&numberOfEntries, sizeof(numberOfEntries));
            entityList.reserve(numberOfEntries);
        }

        while ((entityList.size() < entityList.capacity())) {
            if (ctx->_inBuf->readable() < sizeof(db::EntityList::Entry::_type)) {
                ctx->_rowIndex += i;
                return false;
            }

            entityList.add();

            ctx->_inBuf->readData(&entityList.back()._type, sizeof(db::EntityList::Entry::_type));

            if (ctx->_inBuf->readable() < sizeof(db::EntityList::Entry::_id)) {
                throw TuringException("Entity List Entry can't be broken up across packet/buffer boundaries");
            }

            ctx->_inBuf->readData(&entityList.back()._id, sizeof(db::EntityList::Entry::_id));
        }
    }

    return true;
}

template <typename T>
inline bool decodeOptVector(TuringProtoDecoder::DecodeContext* ctx,
                            db::ColumnOptVector<T>* typedCol,
                            TuringProtoDecoder::DfColumnState* columnState) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptVector");

    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(T)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(T));
            continue;
        }

        T value {};
        ctx->_inBuf->readData(&value, sizeof(T));
        entry = value;
    }
    return true;
}

template <>
inline bool decodeOptVector<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                         db::ColumnOptVector<std::string>* typedCol,
                                         TuringProtoDecoder::DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(uint32_t));
            continue;
        }

        uint32_t stringSize = 0;
        ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

        if (stringSize <= ctx->_inBuf->readable()) {
            entry.emplace(ctx->_inBuf->readPtr(), stringSize);
            ctx->_inBuf->increaseReadOffset(stringSize);
        } else {
            auto& val = entry.emplace(stringSize, '\0');
            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(val.data(), numBytesToRead);

            ctx->_bufferState._start = val.data();
            ctx->_bufferState._len = val.size();
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <>
inline bool decodeOptVector<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                              db::ColumnOptVector<db::types::Embedding::Primitive>* typedCol,
                                                              TuringProtoDecoder::DfColumnState* columnState) {
    for (size_t i = 0; ctx->_rowIndex + i < columnState->_numRows; ++i) {
        if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
            ctx->_rowIndex += i;
            return false;
        }

        auto& entry = (*typedCol)[ctx->_rowIndex + i];

        const bool hasValue = columnState->_bitMask.test(ctx->_rowIndex + i);
        if (!hasValue) {
            ctx->_inBuf->increaseReadOffset(sizeof(uint32_t));
            continue;
        }

        uint32_t embeddingSize = 0;
        ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
        const size_t numFloats = embeddingSize / sizeof(float);

        auto* data = ctx->_embeddingBuffer->alloc(numFloats);
        const std::span<const float> span = ctx->_embeddingBuffer->getSpan(data, numFloats);

        if (embeddingSize <= ctx->_inBuf->readable()) {
            ctx->_inBuf->readData(data, embeddingSize);
            entry.emplace(span);
        } else {
            entry.emplace(span);

            const size_t numBytesToRead = ctx->_inBuf->readable();
            ctx->_inBuf->readData(data, numBytesToRead);

            ctx->_bufferState._start = reinterpret_cast<char*>(data);
            ctx->_bufferState._len = embeddingSize;
            ctx->_bufferState._offset = numBytesToRead;

            ctx->_rowIndex += i + 1;
            return false;
        }
    }
    return true;
}

template <typename T>
inline bool decodeConst(TuringProtoDecoder::DecodeContext* ctx,
                        db::ColumnConst<T>* typedCol) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeConst");

    if (ctx->_inBuf->readable() < sizeof(T)) {
        return false;
    }

    T value {};
    ctx->_inBuf->readData(&value, sizeof(T));
    typedCol->set(value);
    return true;
}

template <>
inline bool decodeConst<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                     db::ColumnConst<std::string>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t stringSize = 0;
    ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

    if (stringSize <= ctx->_inBuf->readable()) {
        std::string val(ctx->_inBuf->readPtr(), stringSize);
        typedCol->set(std::move(val));
        ctx->_inBuf->increaseReadOffset(stringSize);
    } else {
        std::string val(stringSize, '\0');
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(val.data(), numBytesToRead);

        ctx->_bufferState._start = val.data();
        ctx->_bufferState._len = val.size();
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(val));
        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::Path>(TuringProtoDecoder::DecodeContext* ctx,
                                  db::ColumnConst<db::Path>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t pathByteSize = 0;
    ctx->_inBuf->readData(&pathByteSize, sizeof(pathByteSize));
    const size_t numEntities = pathByteSize / sizeof(db::EntityID);

    if (pathByteSize <= ctx->_inBuf->readable()) {
        db::Path path(numEntities);
        ctx->_inBuf->readData(path.data(), pathByteSize);
        typedCol->set(std::move(path));
    } else {
        db::Path path(numEntities);
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(path.data(), numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(path.data());
        ctx->_bufferState._len = pathByteSize;
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(path));
        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                         db::ColumnConst<db::types::Embedding::Primitive>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t embeddingSize = 0;
    ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
    const size_t numFloats = embeddingSize / sizeof(float);

    auto* data = ctx->_embeddingBuffer->alloc(numFloats);
    const std::span<const float> span = ctx->_embeddingBuffer->getSpan(data, numFloats);

    if (embeddingSize <= ctx->_inBuf->readable()) {
        ctx->_inBuf->readData(data, embeddingSize);
        typedCol->set(span);
    } else {
        typedCol->set(span);

        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(data, numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(data);
        ctx->_bufferState._len = embeddingSize;
        ctx->_bufferState._offset = numBytesToRead;

        return false;
    }
    return true;
}

template <>
inline bool decodeConst<db::PropertyNull>(TuringProtoDecoder::DecodeContext* ctx,
                                          db::ColumnConst<db::PropertyNull>* typedCol) {
    return true;
}

template <typename T>
inline bool decodeOptConst(TuringProtoDecoder::DecodeContext* ctx,
                           db::ColumnConst<std::optional<T>>* typedCol) {
    static_assert(TrivialInternalTypes<T>, "Unsupported type for decodeOptConst");

    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(T)) {
        return false;
    }

    T value {};
    ctx->_inBuf->readData(&value, sizeof(T));
    typedCol->set(value);
    return true;
}

template <>
inline bool decodeOptConst<std::string>(TuringProtoDecoder::DecodeContext* ctx,
                                        db::ColumnConst<std::optional<std::string>>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t stringSize = 0;
    ctx->_inBuf->readData(&stringSize, sizeof(stringSize));

    if (stringSize <= ctx->_inBuf->readable()) {
        typedCol->set(std::string(ctx->_inBuf->readPtr(), stringSize));
        ctx->_inBuf->increaseReadOffset(stringSize);
    } else {
        std::string val(stringSize, '\0');
        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(val.data(), numBytesToRead);

        ctx->_bufferState._start = val.data();
        ctx->_bufferState._len = val.size();
        ctx->_bufferState._offset = numBytesToRead;

        typedCol->set(std::move(val));
        return false;
    }
    return true;
}

template <>
inline bool decodeOptConst<db::types::Embedding::Primitive>(TuringProtoDecoder::DecodeContext* ctx,
                                                            db::ColumnConst<std::optional<db::types::Embedding::Primitive>>* typedCol) {
    if (ctx->_inBuf->readable() < sizeof(uint8_t)) {
        return false;
    }

    uint8_t hasValue = 0;
    ctx->_inBuf->readData(&hasValue, sizeof(hasValue));

    if (!hasValue) {
        typedCol->set(std::nullopt);
        return true;
    }

    if (ctx->_inBuf->readable() < sizeof(uint32_t)) {
        return false;
    }

    uint32_t embeddingSize = 0;
    ctx->_inBuf->readData(&embeddingSize, sizeof(embeddingSize));
    const size_t numFloats = embeddingSize / sizeof(float);

    auto* data = ctx->_embeddingBuffer->alloc(numFloats);
    const std::span<const float> span = ctx->_embeddingBuffer->getSpan(data, numFloats);

    if (embeddingSize <= ctx->_inBuf->readable()) {
        ctx->_inBuf->readData(data, embeddingSize);
        typedCol->set(span);
    } else {
        typedCol->set(span);

        const size_t numBytesToRead = ctx->_inBuf->readable();
        ctx->_inBuf->readData(data, numBytesToRead);

        ctx->_bufferState._start = reinterpret_cast<char*>(data);
        ctx->_bufferState._len = embeddingSize;
        ctx->_bufferState._offset = numBytesToRead;

        return false;
    }
    return true;
}

}
