#pragma once

#include <stddef.h>
#include <string>
#include <vector>

#include "Bitmask.h"
#include "TuringProtoHeaders.h"
#include "list/ListBuffer.h"

namespace db {
class Dataframe;
class LocalMemory;
class DataframeManager;
template <typename T>
class ColumnVector;
template <typename T>
class ColumnConst;

}

namespace net::proto {

template <typename T>
class ChunkedBuffer;
class TuringProtoInBuf;

class TuringProtoDecoder {
public:
    struct DfColumnState {
        DynamicLargeBitMask<uint64_t> _bitMask {0};
        uint32_t _numRows {0};

        void reset() {
            _bitMask.resize(0);
            _numRows = 0;
        }
    };

    struct DecodedColumnSchema {
        net::proto::ColumnWireHeader _header;
        std::string _colName;
        DfColumnState _colState;
    };

    struct InterruptedBufferState {
        char* _start {nullptr};
        size_t _len {0};
        size_t _offset {0};

        void reset() {
            _start = nullptr;
            _len = 0;
            _offset = 0;
        }
    };

    struct DecodeContext {
        size_t _colIndex {0};
        size_t _rowIndex {0};
        TuringProtoInBuf* _inBuf {nullptr};
        InterruptedBufferState _bufferState;
        ChunkedBuffer<float>* _embeddingBuffer {nullptr};
        ChunkedBuffer<char>* _stringBuffer {nullptr};
        db::ListBuffer<>* _listBuffer {nullptr};

        // Cursor for the list row currently being decoded: how many elements it has in
        // total and how many we have written so far. Lets a list resume across buffer
        // boundaries; the elements themselves live in the (already emplaced) ListView.
        uint32_t _listCount {0};
        uint32_t _listWritten {0};

        void reset() {
            _colIndex = 0;
            _rowIndex = 0;
            _bufferState.reset();
            _listCount = 0;
            _listWritten = 0;
        }
    };

    TuringProtoDecoder(db::LocalMemory* localMem,
                       db::DataframeManager* dfMan,
                       TuringProtoInBuf* inBuf,
                       ChunkedBuffer<float>* embeddingBuffer,
                       ChunkedBuffer<char>* stringBuffer,
                       db::ListBuffer<>* listBuffer);

    void decodeIncomingChunkHeader(db::Dataframe* df,
                                   std::vector<DecodedColumnSchema>& colSchemas);
    void decodeIncomingChunk(db::Dataframe* df,
                             std::vector<DecodedColumnSchema>& colSchemas);
    void decodeIncomingData(db::Dataframe* df,
                            std::vector<DecodedColumnSchema>& colSchemas);

    void reset();

    template <typename T>
    bool decodeColumn(db::ColumnVector<T>* col, DfColumnState* colState);

    template <typename T>
    bool decodeColumn(db::ColumnVector<std::optional<T>>* col, DfColumnState* colState);

    template <typename T>
    bool decodeColumn(db::ColumnConst<T>* col);

    template <typename T>
    bool decodeColumn(db::ColumnConst<std::optional<T>>* col);

private:
    db::LocalMemory* _localMem {nullptr};
    db::DataframeManager* _dfMan {nullptr};
    DecodeContext _ctxt;
};

}
