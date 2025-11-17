#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "columns/ColumnOptVector.h"
#include "metadata/SupportedType.h"

namespace db {

template <typename T>
concept IteratedID = std::is_same_v<T, NodeID> || std::is_same_v<T, EdgeID>;

template <IteratedID ID, SupportedType T>
class GetPropertiesIterator : public Iterator {
public:
    using ColumnIDs = ColumnVector<ID>;

    GetPropertiesIterator(const GraphView& view,
                          PropertyTypeID propTypeID,
                          const ColumnIDs* inputIDs);

    void init();
    void reset();

    void next() override;

    const T::Primitive& get() const {
        return *_prop;
    }

    ID getCurrentEntityID() const {
        return *_entityIt;
    }

    GetPropertiesIterator& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

private:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};

protected:
    ColumnIDs::ConstIterator _entityIt;
    const ColumnIDs* _inputIDs {nullptr};
};

template <IteratedID ID, SupportedType T>
struct GetPropertiesRange {
    using ColumnIDs = ColumnVector<ID>;

    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const ColumnIDs* _inputIDs {nullptr};

    GetPropertiesIterator<ID, T> begin() const { return {_view, _propTypeID, _inputIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
};

template <IteratedID ID, SupportedType T>
class GetPropertiesIteratorWithNull : public Iterator {
public:
    using ColumnIDs = ColumnVector<ID>;

    GetPropertiesIteratorWithNull(const GraphView& view,
                                  PropertyTypeID propTypeID,
                                  const ColumnIDs* inputIDs);

    bool isValid() const override {
        return _entityIt != _inputIDs->end();
    }

    void reset();

    void next() override;

    const T::Primitive* get() const {
        return _prop;
    }

    ID getCurrentID() const {
        return *_entityIt;
    }

    GetPropertiesIteratorWithNull& operator++() {
        next();
        return *this;
    }

    const T::Primitive* operator*() const {
        return get();
    }

protected:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};
    const ColumnIDs* _inputIDs {nullptr};
    ColumnIDs::ConstIterator _entityIt;

    void init();
};

template <IteratedID ID, SupportedType T>
class GetPropertiesChunkWriter : public GetPropertiesIterator<ID, T> {
public:
    using IDType = ID;
    using PropertyType = T;
    using ValueType = typename T::Primitive;
    using ColumnIDs = ColumnVector<ID>;
    using ColumnValues = ColumnVector<ValueType>;

    GetPropertiesChunkWriter(const GraphView& view,
                             PropertyTypeID propTypeID,
                             const ColumnIDs* inputIDs);

    void fill(size_t maxCount);

    void setOutput(ColumnValues* output) { _output = output; }
    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }

private:
    ColumnVector<size_t>* _indices {nullptr};
    ColumnValues* _output {nullptr};
};

template <IteratedID ID, SupportedType T>
class GetPropertiesWithNullChunkWriter : public GetPropertiesIteratorWithNull<ID, T> {
public:
    using ColumnIDs = ColumnVector<ID>;
    using ColumnValues = ColumnOptVector<typename T::Primitive>;

    GetPropertiesWithNullChunkWriter(const GraphView& view,
                                     PropertyTypeID propTypeID,
                                     const ColumnIDs* inputIDs);

    void fill(size_t maxCount);

    void setOutput(ColumnValues* output) { _output = output; }

private:
    ColumnValues* _output {nullptr};
};

template <SupportedType T>
using GetNodePropertiesIterator = GetPropertiesIterator<NodeID, T>;
template <SupportedType T>
using GetNodePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<NodeID, T>;
template <SupportedType T>
using GetNodePropertiesRange = GetPropertiesRange<NodeID, T>;
template <SupportedType T>
using GetNodePropertiesChunkWriter = GetPropertiesChunkWriter<NodeID, T>;
template <SupportedType T>
using GetNodePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<NodeID, T>;

template <SupportedType T>
using GetEdgePropertiesIterator = GetPropertiesIterator<EdgeID, T>;
template <SupportedType T>
using GetEdgePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<EdgeID, T>;
template <SupportedType T>
using GetEdgePropertiesRange = GetPropertiesRange<EdgeID, T>;
template <SupportedType T>
using GetEdgePropertiesChunkWriter = GetPropertiesChunkWriter<EdgeID, T>;
template <SupportedType T>
using GetEdgePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<EdgeID, T>;

}
