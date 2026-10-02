#pragma once

#include <stddef.h>
#include <algorithm>
#include <optional>
#include <type_traits>

#include "NumpyColumn.h"
#include "NumpyColumnConst.h"
#include "NumpyColumnContainer.h"
#include "NumpyColumnVector.h"
#include "NumpyMaskedColumnVector.h"

#include "DecodeContext.h"
#include "DecodedColumnSchema.h"
#include "Decoders.h"
#include "TuringProtoDecoderConcepts.h"
#include "TuringSink.h"

namespace pybindings {

// Decodes a response straight into NumPy-owned memory. Strings, embeddings, lists and maps
// are stored as TuringSink stores them; only the columns differ.
class NumpySink : public net::proto::TuringSink {
public:
    using ColumnContainer = NumpyColumnContainer;
    using Column = NumpyColumn;

    template <typename T>
    using ColumnVector = NumpyColumnVector<T>;

    template <typename T>
    using ColumnOptVector = std::conditional_t<net::proto::TrivialInternalTypes<T>,
                                               NumpyMaskedColumnVector<T>,
                                               NumpyColumnVector<std::optional<T>>>;

    template <typename T>
    using ColumnConst = NumpyColumnConst<T>;

    template <typename T>
    using ColumnOptConst = NumpyColumnConst<std::optional<T>>;

    NumpySink(db::LocalMemory* localMemory,
              net::proto::ChunkedBuffer<float>* embeddingBuffer,
              db::ListBuffer<>* listBuffer,
              db::MapBuffer<>* mapBuffer);
    ~NumpySink();

    template <typename ColumnT>
    Column* alloc() { return new ColumnT(); }
};

}

namespace net::proto {

// Every row's value is on the wire, null or not, so the values are copied in bulk. The null
// bitmask is already read in full before the values.
template <typename T>
    requires TrivialInternalTypes<T>
struct OptionalVectorColumnDecoder<T, pybindings::NumpySink> {
    static bool decode(DecodeContext* context,
                       pybindings::NumpySink* sink,
                       pybindings::NumpyMaskedColumnVector<T>* typedColumn,
                       ProtoColumnState* columnState) {
        const size_t numRows = columnState->getNumRows();
        const size_t rowsReadable = std::min(numRows - context->_rowIndex, context->_inBuf->readable() / sizeof(T));

        context->_inBuf->readData(typedColumn->getValues() + context->_rowIndex, rowsReadable * sizeof(T));
        context->_rowIndex += rowsReadable;

        if (context->_rowIndex < numRows) {
            return false;
        }

        const ProtoColumnState::BitMask& bitMask = columnState->getBitMask();
        bool* mask = typedColumn->getMask();

        for (size_t row = 0; row < numRows; row++) {
            mask[row] = !bitMask.test(row);
        }

        return true;
    }
};

}
