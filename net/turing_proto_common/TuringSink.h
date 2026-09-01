#pragma once

#include <stddef.h>
#include <deque>
#include <optional>
#include <span>
#include <string_view>

#include "LocalMemory.h"
#include "list/ListBuffer.h"

namespace db {
class Column;
template <typename T>
class ColumnVector;
template <typename T>
class ColumnConst;
}

namespace net::proto {
template <typename T>
class ChunkedBuffer;
class TuringProtoInBuf;
class TuringSinkColumnContainer;

class TuringSink {
public:
    using ColumnContainer = TuringSinkColumnContainer;

    using Column = db::Column;

    template<typename T>
    using ColumnVector = db::ColumnVector<T>;

    template<typename T>
    using ColumnOptVector = db::ColumnVector<std::optional<T>>;

    template<typename T>
    using ColumnConst = db::ColumnConst<T>;

    template<typename T>
    using ColumnOptConst = db::ColumnConst<std::optional<T>>;

    TuringSink() = delete;
    TuringSink(db::LocalMemory* localMemory,
               ChunkedBuffer<float>* embeddingBuffer,
               ChunkedBuffer<char>* stringBuffer,
               db::ListBuffer<>* listBuffer);
    ~TuringSink();

    float* allocEmbedding(size_t numFloats);
    std::span<const float> getEmbeddingView(float* data, size_t numFloats);

    char* allocString(size_t size);
    std::string_view getStringView(char* data, size_t size);

    // Member template: defined here (not the .cpp) so instantiations in other
    // translation units can see the definition.
    template<typename T>
    Column* alloc() { return _localMemory->alloc<T>(); }

    // List builder surface: the decoder reports list structure as it comes off the
    // wire; this family stores elements as tagged bytes in the list arena behind a
    // stack of write cursors (one per open nesting level, resumable across chunks).
    db::ListView beginList(size_t elementCount, size_t byteSize);
    db::ListElementView beginNestedList(size_t elementCount, size_t byteSize);

    db::ListElementView writeListValue(std::string_view value);
    db::ListElementView writeListValue(std::span<const float> value);
    db::ListElementView writeListElementBytes(const char* bytes, size_t byteSize);

    bool hasOpenList() const { return !_listStack.empty(); }
    bool topListComplete() const { return _listStack.back().isComplete(); }
    void popList() { _listStack.pop_back(); }
    size_t openListCount() const { return _listStack.size(); }
    size_t topLevelElementsWritten() const { return _listStack.front().getWritten(); }

    void reset();

private:
    db::LocalMemory* _localMemory {nullptr};
    ChunkedBuffer<float>* _embeddingBuffer {nullptr};
    ChunkedBuffer<char>* _stringBuffer {nullptr};
    db::ListBuffer<>* _listBuffer {nullptr};

    // Front is the column's own (top-level) cursor; back is the innermost open list.
    std::deque<db::ListWriteCursor> _listStack;
};
}
