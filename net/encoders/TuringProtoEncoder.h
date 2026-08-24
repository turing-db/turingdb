#pragma once

#include <span>
#include <stack>
#include <type_traits>
#include <utility>

#include "OutputValues.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "list/ListUtils.h"
#include "QueryCallbacks.h"
#include "Bitmask.h"
#include "spdlog/spdlog.h"

namespace db {
class QueryStatus;
}

namespace net::proto {

struct NestedListStackElement {
    std::span<const db::ListElementView>::iterator currIt;
    std::span<const db::ListElementView>::iterator endIt;
};

/// Dispatched on a list element's runtime tag to return the size of the object
/// the tag maps to, so the client knows how much to allocate. For String and
/// Embedding this is the size of the view object (std::string_view / std::span),
/// not the length of the data it points to.
struct ListElementByteSizeVisitor {
    template <typename T>
    size_t operator()(const db::ListElementView elem) const {
        return sizeof(T);
    }
};

// Writes one list as [count][listByteSize] followed by its [tag][value] /
// [tag][numBytes][data] elements. Shared by the vector and constant paths.
// Σ sizeof(value) over the elements — the deserialized footprint the decoder reserves.
inline WireSize computeListByteSize(std::span<const db::ListElementView> elements) {
    const ListElementByteSizeVisitor sizeVisitor;

    size_t totalSize = 0;
    for (const auto& elem : elements) {
        db::ListTagDispatcher dispatcher {elem.getTag()};
        totalSize += dispatcher.execute(sizeVisitor, elem);
    }

    bioassert(totalSize <= MAX_WIRE_SIZE, "List length exceeds maximum wire size");
    return static_cast<WireSize>(totalSize);
}

struct ColInternalKindToProtoEnum {
    template <typename T>
    static constexpr auto map() {
        using Enum = net::proto::ColumnInternalKind;
        if constexpr (std::is_same_v<T, db::NodeID>) {
            return Enum::NODE_ID;
        } else if constexpr (std::is_same_v<T, db::EdgeID>) {
            return Enum::EDGE_ID;
        } else if constexpr (std::is_same_v<T, db::EdgeTypeID>) {
            return Enum::EDGE_TYPE_ID;
        } else if constexpr (std::is_same_v<T, db::PropertyTypeID>) {
            return Enum::PROPERTY_TYPE_ID;
        } else if constexpr (std::is_same_v<T, db::LabelID>) {
            return Enum::LABEL_ID;
        } else if constexpr (std::is_same_v<T, db::LabelSetID>) {
            return Enum::LABEL_SET_ID;
        } else if constexpr (std::is_same_v<T, db::CommitHash>) {
            return Enum::COMMIT_HASH;
        } else if constexpr (std::is_same_v<T, db::ChangeID>) {
            return Enum::CHANGE_ID;
        } else if constexpr (std::unsigned_integral<T> || db::OptionalUnsignedInteger<T>) {
            return Enum::UINT64;
        } else if constexpr (db::IsInt64<T>) {
            return Enum::INT64;
        } else if constexpr (db::IsFloat64<T>) {
            return Enum::DOUBLE;
        } else if constexpr (db::IsString<T>) {
            return Enum::STRING;
        } else if constexpr (db::IsBool<T>) {
            return Enum::BOOL;
        } else if constexpr (db::IsPath<T>) {
            return Enum::PATH;
        } else if constexpr (db::IsEmbedding<T>) {
            return Enum::EMBEDDING;
        } else if constexpr (db::IsEntityList<T>) {
            return Enum::ENTITY_LIST;
        } else if constexpr (db::IsListView<T>) {
            return Enum::LIST_VIEW;
        } else if constexpr (db::IsListElement<T>) {
            return Enum::LIST_ELEMENT_VIEW;
        } else if constexpr (db::IsValueType<T>) {
            return Enum::VALUE_TYPE;
        } else if constexpr (db::IsNull<T>) {
            return Enum::PROPERTY_NULL;
        } else {
            static_assert(sizeof(T) == 0, "No mapping for this type");
        }
    }
};

struct ColumnHeaderWriter {
    net::proto::ColumnWireHeader& _header;

    ColumnHeaderWriter() = delete;
    explicit ColumnHeaderWriter(net::proto::ColumnWireHeader& header)
        : _header(header)
    {
    }

    void writeColumnSchema(net::proto::ColumnInternalKind typeCode,
                           net::proto::ColumnKind encoding) {
        _header._typeCode= std::to_underlying(typeCode);
        _header._encoding = std::to_underlying(encoding);
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::VECTOR);
    }

    template <typename T>
    void operator()(const db::ColumnVector<std::optional<T>>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::OPTIONAL_VECTOR);
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::CONSTANT);
    }

    template <typename T>
    void operator()(const db::ColumnConst<std::optional<T>>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::OPTIONAL_CONSTANT);
    }
};


/// Dispatched on a list element's runtime tag to write [tag][value] for fixed types, or
/// [tag][numBytes][data] for variable-length types (String, Embedding). The tag and the
/// fixed framing are kept within a single chunk (checkRemainingAndFlush) so the decoder,
/// which reads each unit atomically, never sees them split across a packet boundary; the
/// variable payload that follows still streams across chunks via copyVarLenData.
struct ListElementWriteVisitor {
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    std::stack<NestedListStackElement>* _stack {nullptr};

    template <typename T>
    void operator()(const db::ListElementView elem) const {
        constexpr size_t tagSize = sizeof(db::ListBufferTypeTag);
        const db::ListBufferTypeTag tag = db::TypeToListBufferTag<T>::Tag;

        if constexpr (db::StringLike<T>) {
            const T value = elem.getAs<T>();
            bioassert(value.size() * sizeof(char) <= MAX_WIRE_SIZE, "List string element exceeds maximum wire size");
            const WireSize numBytes = static_cast<WireSize>(value.size() * sizeof(char));

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(numBytes));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&numBytes, sizeof(numBytes));
            _outBuf->copyVarLenData(value.data(), numBytes);
        } else if constexpr (db::IsEmbedding<T>) {
            const T value = elem.getAs<T>();
            bioassert(value.size() * sizeof(float) <= MAX_WIRE_SIZE, "List embedding element exceeds maximum wire size");
            const WireSize numBytes = static_cast<WireSize>(value.size() * sizeof(float));

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(numBytes));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&numBytes, sizeof(numBytes));
            _outBuf->copyVarLenData(value.data(), numBytes);
        } else if constexpr (db::IsListView<T>) {
            const T value = elem.getAs<T>();
            const auto tag = elem.getTag();

            bioassert(value.size() <= MAX_WIRE_SIZE, "List element count exceeds maximum wire size");
            const WireSize elementCount = static_cast<WireSize>(value.size());
            const WireSize listByteSize = computeListByteSize(value);

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(elementCount) + sizeof(listByteSize));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&elementCount, sizeof(elementCount));
            _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

            _stack->emplace(value.begin(), value.end());
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "ListViewEncoder can't encode a non trivial element in a trivial manner");
            const T value = elem.getAs<T>();

            _outBuf->checkRemainingAndFlush(tagSize + sizeof(T));
            _outBuf->copyFixedLenData(&tag, tagSize);
            _outBuf->copyFixedLenData(&value, sizeof(T));
        }
    }
};

class DataWriter {
public:
    explicit DataWriter(net::proto::TuringProtoOutBuf* outBuf, std::stack<NestedListStackElement>& stack)
        : _outBuf(outBuf),
        _stack(stack)
    {
    }

    ~DataWriter() = default;

    void writeRowCount(size_t size) {
        bioassert(size <= MAX_WIRE_SIZE, "Number of data frame rows is too high");
        const WireSize rowCount = static_cast<WireSize>(size);
        _outBuf->copyFixedLenData(&rowCount, sizeof(rowCount));
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* col) {
        writeRowCount(col->size());

        if constexpr (db::StringLike<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(char) <= MAX_WIRE_SIZE, "String length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsPath<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(db::EntityID) <= MAX_WIRE_SIZE, "Path length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(db::EntityID));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(float) <= MAX_WIRE_SIZE, "Embedding length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsEntityList<T>) {
            constexpr size_t sizeOfEntry = sizeof(db::EntityList::Entry::_id) + sizeof(db::EntityList::Entry::_type);

            for (const auto& entityList : *col) {
                bioassert(entityList.size() <= MAX_WIRE_SIZE, "Entity list length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(entityList.size());
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                for (const auto& entry : entityList) {
                    // Ensure that we can copy a full entry into the packet
                    _outBuf->checkRemainingAndFlush(sizeOfEntry);

                    _outBuf->copyFixedLenData(&entry._type, sizeof(entry._type));
                    _outBuf->copyFixedLenData(&entry._id, sizeof(entry._id));
                }
            }
        } else if constexpr (db::IsListView<T>) {
            for (const auto& listView : *col) {
                writeListView(listView);
            }
        } else if constexpr (db::IsListElement<T>) {
            // One element per row: the row count (already written above) is the element
            // count, so only [listByteSize] + the elements follow.
            writeListElements(col->getRaw());
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder can't encode a non trivial element in a trivial manner");
            const size_t columnByteSize = sizeof(T) * col->size();
            _outBuf->copyVector<T>(col->data(), columnByteSize);
        }
    }

    template <typename T>
    void operator()(const db::ColumnVector<std::optional<T>>* col) {
        writeRowCount(col->size());

        DynamicLargeBitMask<uint64_t> mask(0);
        DynamicLargeBitMask<uint64_t>::create(mask, col->getRaw());
        _outBuf->copyVarLenData(mask.data(), mask.byteSize());

        if constexpr (db::StringLike<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const WireSize columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(char) <= MAX_WIRE_SIZE, "Optional string length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val->size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsPath<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const WireSize columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(db::EntityID) <= MAX_WIRE_SIZE, "Optional path length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val->size() * sizeof(db::EntityID));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const WireSize columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(float) <= MAX_WIRE_SIZE, "Optional embedding length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val->size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsEntityList<T>) {
            // EntityList is only used as ColumnVector<EntityList>, never optional.
            // sizeof(T) == 0 (never true) keeps the assert dependent on T so it only
            // fires if this branch is ever instantiated; a bare static_assert(false)
            // is diagnosed eagerly by pre-P2593 compilers even when discarded.
            static_assert(sizeof(T) == 0, "Sending ColumnOptVector<EntityList> not supported");
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const T zero {};
                    _outBuf->copyFixedLenData(&zero, sizeof(T));
                    continue;
                }

                _outBuf->copyFixedLenData(&*val, sizeof(T));
            }
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        if constexpr (db::StringLike<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(char) <= MAX_WIRE_SIZE, "Const string length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsPath<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(db::EntityID) <= MAX_WIRE_SIZE, "Const path length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(db::EntityID));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEmbedding<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(float) <= MAX_WIRE_SIZE, "Const embedding length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEntityList<T>) {
            // It is not used/returned anywhere in the codebase so we disable support for
            // it now. We can renable it if ever needed.
            throw FatalException("ColumnConst<EntityList> is not supported");
        } else if constexpr (db::IsListView<T>) {
            writeListView(col->at(0));
        } else if constexpr (db::IsListElement<T>) {
            const db::ListElementView element = col->at(0);
            writeListElements(std::span<const db::ListElementView>(&element, 1));
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            _outBuf->copyFixedLenData(&col->at(0), sizeof(T));
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<std::optional<T>>* col) {
        const auto& opt = col->at(0);
        const uint8_t hasValue = opt.has_value() ? 1 : 0;
        _outBuf->copyFixedLenData(&hasValue, sizeof(hasValue));

        if (!hasValue) {
            return;
        }

        if constexpr (db::StringLike<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(char) <= MAX_WIRE_SIZE, "Optional const string length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsPath<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(db::EntityID) <= MAX_WIRE_SIZE, "Optional const path length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(db::EntityID));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEmbedding<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(float) <= MAX_WIRE_SIZE, "Optional const embedding length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEntityList<T>) {
            // EntityList is only used as ColumnVector<EntityList>, never optional.
            // Dependent condition (see the ColumnOptVector<EntityList> branch above).
            static_assert(sizeof(T) == 0, "ColumnOptConst<EntityList> is not supported");
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            _outBuf->copyFixedLenData(&*opt, sizeof(T));
        }
    }

private:
    // Writes each element's [tag][value] / [tag][numBytes][data].
    void writeListElementValues(std::span<const db::ListElementView> elements) {
        const ListElementWriteVisitor writeVisitor {_outBuf, &_stack};

        _stack.emplace(elements.begin(), elements.end());

        // Depth-first walk over the (possibly nested) list via an explicit stack: each frame
        // resumes from its saved iterator, and a nested list pushes a child frame that is
        // fully drained before the parent continues.
        while (!_stack.empty()) {
            auto& [currIt, endIt] = _stack.top();
            if (currIt == endIt) {
                _stack.pop();
                continue;
            }

            db::ListTagDispatcher {currIt->getTag()}.execute(writeVisitor, *currIt);
            ++currIt;
        }
    }

    // A whole list value: [count][listByteSize] followed by the elements.
    void writeListView(const db::ListView& listView) {
        const std::span<const db::ListElementView> elements = listView.elements();

        bioassert(elements.size() <= MAX_WIRE_SIZE, "List element count exceeds maximum wire size");
        const WireSize elementCount = static_cast<WireSize>(elements.size());
        const WireSize listByteSize = computeListByteSize(elements);

        // Keep the [count][listByteSize] header together in one chunk.
        _outBuf->checkRemainingAndFlush(sizeof(elementCount) + sizeof(listByteSize));
        _outBuf->copyFixedLenData(&elementCount, sizeof(elementCount));
        _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

        writeListElementValues(elements);
    }

    // Helper function to write ColumnVectors or Column Const List Element Views.
    void writeListElements(std::span<const db::ListElementView> elements) {
        const WireSize listByteSize = computeListByteSize(elements);

        _outBuf->checkRemainingAndFlush(sizeof(listByteSize));
        _outBuf->copyFixedLenData(&listByteSize, sizeof(listByteSize));

        writeListElementValues(elements);
    }

    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    std::stack<NestedListStackElement>& _stack;
};

class TuringProtoEncoder {
public:
    explicit TuringProtoEncoder(net::proto::TuringProtoOutBuf* outBuf);

    void writeDataframeHeader(const db::Dataframe* df);
    void writeDataframe(const db::Dataframe* df);
    void writeError(const db::QueryStatus* status);
    void writeProtocolError(std::string_view message);
    void writeEnd(db::QueryCallbacks::ExecTimeMilliseconds milliseconds);

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    std::stack<NestedListStackElement> _stack;
};

}
