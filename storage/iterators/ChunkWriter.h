#pragma once

#include "columns/ColumnVector.h"

namespace db {

class Iterator;
class NodeView;

template <typename T>
concept ChunkWriter = requires(T it) {
    { std::derived_from<T, Iterator> };
    { it.fill(0) };
};

// TODO Comment
template <typename T>
concept NonRootChunkWriter = requires(T it) {
    { it.setIndices(new ColumnVector<size_t>) };
};

template <typename T>
concept NodeIDsChunkWriter = requires(T it) {
    { it.setNodeIDs(new ColumnVector<NodeID>) };
};

template <typename T>
concept TgtIDsChunkWriter = requires(T it) {
    { it.setTgtIDs(new ColumnVector<NodeID>) };
};

template <typename T>
concept SrcIDsChunkWriter = requires(T it) {
    { it.setSrcIDs(new ColumnVector<NodeID>) };
};

template <typename T>
concept OtherIDsChunkWriter = requires(T it) {
    { it.setOtherIDs(new ColumnVector<NodeID>) };
};

template <typename T>
concept EdgeIDsChunkWriter = requires(T it) {
    { it.setEdgeIDs(new ColumnVector<EdgeID>) };
};

template <typename T>
concept EdgeTypesChunkWriter = requires(T it) {
    { it.setEdgeTypes(new ColumnVector<EdgeTypeID>) };
};

template <typename T>
concept PropertiesChunkWriter = requires(T it) {
    { it.setProperties(new ColumnVector<typename T::Primitive>) };
};

template <typename T>
concept NodeViewChunkWriter = requires(T it) {
    { it.setNodeViewsColumn(new ColumnVector<NodeView>) };
};

}
