#include "NLWrittenValues.h"

#include <type_traits>
#include <variant>

using namespace db;

namespace {

// A column of strings or embeddings borrows what it holds, and the change rewrites its
// own values as the query runs, so those are copied where the column can outlive them
template <SupportedType T>
const NLWrittenValues::Value& retainIfBorrowed(NLWrittenValues& written, const NLWrittenValues::Value& value) {
    if constexpr (std::is_same_v<T, types::String> || std::is_same_v<T, types::Embedding>) {
        return written.retain(value);
    } else {
        return value;
    }
}

}

NLWrittenValues::NLWrittenValues() {
}

NLWrittenValues::~NLWrittenValues() {
}

void NLWrittenValues::indexUpdates(const CommitWriteBuffer* writeBuffer) {
    _writeBuffer = writeBuffer;

    const CommitWriteBuffer::UpdatedNodes& nodes = writeBuffer->updatedNodes();
    for (; _indexedNodeUpdates < nodes.size(); _indexedNodeUpdates++) {
        const CommitWriteBuffer::NodeUpdate& update = nodes[_indexedNodeUpdates];
        const Key key {._entity=update._idToUpdate.getValue(),
                       ._property=update._updatedValue.propertyID.getValue()};

        _nodeUpdates[key] = _indexedNodeUpdates;
    }

    const CommitWriteBuffer::UpdatedEdges& edges = writeBuffer->updatedEdges();
    for (; _indexedEdgeUpdates < edges.size(); _indexedEdgeUpdates++) {
        const CommitWriteBuffer::EdgeUpdate& update = edges[_indexedEdgeUpdates];
        const Key key {._entity=update._idToUpdate.getValue(),
                       ._property=update._updatedValue.propertyID.getValue()};

        _edgeUpdates[key] = _indexedEdgeUpdates;
    }
}

const NLWrittenValues::Value* NLWrittenValues::findNodeUpdate(NodeID node, PropertyTypeID property) const {
    const Key key {._entity=node.getValue(), ._property=property.getValue()};

    const auto findIt = _nodeUpdates.find(key);
    if (findIt == end(_nodeUpdates)) {
        return nullptr;
    }

    return &_writeBuffer->updatedNodes()[findIt->second]._updatedValue.value;
}

const NLWrittenValues::Value* NLWrittenValues::findEdgeUpdate(EdgeID edge, PropertyTypeID property) const {
    const Key key {._entity=edge.getValue(), ._property=property.getValue()};

    const auto findIt = _edgeUpdates.find(key);
    if (findIt == end(_edgeUpdates)) {
        return nullptr;
    }

    return &_writeBuffer->updatedEdges()[findIt->second]._updatedValue.value;
}

template <TypedInternalID IDT>
const NLWrittenValues::Value* NLWrittenValues::findUpdate(IDT entity, PropertyTypeID property) const {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        return findNodeUpdate(entity, property);
    } else {
        return findEdgeUpdate(entity, property);
    }
}

template <SupportedType T>
std::optional<typename T::Primitive> NLWrittenValues::read(const Value& value) {
    using Primitive = typename T::Primitive;

    const auto convert = [this](const auto& held) -> std::optional<Primitive> {
        using Inner = typename std::decay_t<decltype(held)>::value_type;

        constexpr bool isEncodedList = std::is_same_v<T, types::List> && std::is_same_v<Inner, EncodedList>;
        constexpr bool isEncodedMap = std::is_same_v<T, types::Map> && std::is_same_v<Inner, EncodedMap>;

        if constexpr (isEncodedList || isEncodedMap) {
            if (!held) {
                return std::nullopt;
            }

            return decode(*held);
        } else if constexpr (std::is_convertible_v<const Inner&, Primitive>) {
            if (!held) {
                return std::nullopt;
            }

            return Primitive(*held);
        } else {
            return std::nullopt;
        }
    };

    return std::visit(convert, retainIfBorrowed<T>(*this, value));
}

const NLWrittenValues::Value& NLWrittenValues::retain(const Value& value) {
    return _retained.emplace_back(value);
}

ListView NLWrittenValues::decode(const EncodedList& list) {
    return list.decodeInto(_decodedLists);
}

MapView NLWrittenValues::decode(const EncodedMap& map) {
    return map.decodeInto(_decodedMaps);
}

namespace db {

template const NLWrittenValues::Value* NLWrittenValues::findUpdate<NodeID>(NodeID entity, PropertyTypeID property) const;
template const NLWrittenValues::Value* NLWrittenValues::findUpdate<EdgeID>(EdgeID entity, PropertyTypeID property) const;

template std::optional<types::Int64::Primitive> NLWrittenValues::read<types::Int64>(const Value& value);
template std::optional<types::UInt64::Primitive> NLWrittenValues::read<types::UInt64>(const Value& value);
template std::optional<types::Double::Primitive> NLWrittenValues::read<types::Double>(const Value& value);
template std::optional<types::String::Primitive> NLWrittenValues::read<types::String>(const Value& value);
template std::optional<types::Bool::Primitive> NLWrittenValues::read<types::Bool>(const Value& value);
template std::optional<types::Embedding::Primitive> NLWrittenValues::read<types::Embedding>(const Value& value);
template std::optional<types::List::Primitive> NLWrittenValues::read<types::List>(const Value& value);
template std::optional<types::Map::Primitive> NLWrittenValues::read<types::Map>(const Value& value);
template std::optional<types::DateTime::Primitive> NLWrittenValues::read<types::DateTime>(const Value& value);

}
