#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "types/SupportedType.h"

namespace db {

struct GetNodePropertiesIteratorTag {};
struct GetEdgePropertiesIteratorTag {};

template <typename IteratorTag, SupportedType T>
class GetPropertiesIterator : public Iterator {
public:
    GetPropertiesIterator(const GraphView& view,
                          PropertyTypeID propTypeID,
                          const ColumnIDs* inputEntityIDs);

    void next() override;

    const T::Primitive& get() const {
        return *_prop;
    }

    EntityID getCurrentEntityID() const {
        return *_entityIt;
    }

    GetPropertiesIterator<IteratorTag, T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

private:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};
    const ColumnIDs* _inputEntityIDs {nullptr};
    ColumnIDs::ConstIterator _entityIt;
};

template <typename IteratorTag, SupportedType T>
struct GetPropertiesRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const ColumnIDs* _inputEntityIDs {nullptr};

    GetPropertiesIterator<IteratorTag, T> begin() const { return {_view, _propTypeID, _inputEntityIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
};

template <typename IteratorTag, SupportedType T>
class GetPropertiesIteratorWithNull : public Iterator {
public:
    GetPropertiesIteratorWithNull(const GraphView& view,
                                  PropertyTypeID propTypeID,
                                  const ColumnIDs* inputEntityIDs);

    bool isValid() const override {
        return _entityIt != _inputEntityIDs->end();
    }

    void reset();

    void next() override;

    const T::Primitive* get() const {
        return _prop;
    }

    EntityID getCurrentEntityID() const {
        return *_entityIt;
    }

    GetPropertiesIteratorWithNull<IteratorTag, T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive* operator*() const {
        return get();
    }

protected:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};
    const ColumnIDs* _inputEntityIDs {nullptr};
    ColumnIDs::ConstIterator _entityIt;

    void init();
};

template <typename IteratorTag, SupportedType T>
class GetPropertiesWithNullChunkWriter : public GetPropertiesIteratorWithNull<IteratorTag, T> {
public:
    GetPropertiesWithNullChunkWriter(const GraphView& view,
                                     PropertyTypeID propTypeID,
                                     const ColumnIDs* inputEntityIDs);

    void fill(size_t maxCount);

    void setOutput(ColumnOptVector<typename T::Primitive>* output) { _output = output; }

private:
    ColumnOptVector<typename T::Primitive>* _output {nullptr};
};

template <SupportedType T>
using GetNodePropertiesIterator = GetPropertiesIterator<GetNodePropertiesIteratorTag, T>;
template <SupportedType T>
using GetNodePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<GetNodePropertiesIteratorTag, T>;
template <SupportedType T>
using GetNodePropertiesRange = GetPropertiesRange<GetNodePropertiesIteratorTag, T>;
template <SupportedType T>
using GetNodePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<GetNodePropertiesIteratorTag, T>;

template <SupportedType T>
using GetEdgePropertiesIterator = GetPropertiesIterator<GetEdgePropertiesIteratorTag, T>;
template <SupportedType T>
using GetEdgePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<GetEdgePropertiesIteratorTag, T>;
template <SupportedType T>
using GetEdgePropertiesRange = GetPropertiesRange<GetEdgePropertiesIteratorTag, T>;
template <SupportedType T>
using GetEdgePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<GetEdgePropertiesIteratorTag, T>;

}
