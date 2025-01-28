#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "types/SupportedType.h"

namespace db {

template <SupportedType T>
class GetNodePropertiesIterator : public Iterator {
public:
    GetNodePropertiesIterator(const GraphView& view,
                              PropertyTypeID propTypeID,
                              const ColumnIDs* inputNodeIDs);

    void next() override;

    const T::Primitive& get() const {
        return *_prop;
    }

    EntityID getCurrentNodeID() const {
        return *_nodeIt;
    }

    GetNodePropertiesIterator<T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

private:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};
    const ColumnIDs* _inputNodeIDs {nullptr};
    ColumnIDs::ConstIterator _nodeIt;
};

template <SupportedType T>
struct GetNodePropertiesRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const ColumnIDs* _inputNodeIDs {nullptr};

    GetNodePropertiesIterator<T> begin() const { return {_view, _propTypeID, _inputNodeIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
};

template <SupportedType T>
class GetNodePropertiesIteratorWithNull : public Iterator {
public:
    GetNodePropertiesIteratorWithNull(const GraphView& view,
                                      PropertyTypeID propTypeID,
                                      const ColumnIDs* inputNodeIDs);

    bool isValid() const override {
        return _nodeIt != _inputNodeIDs->end();
    }

    void reset();

    void next() override;

    const T::Primitive* get() const {
        return _prop;
    }

    EntityID getCurrentNodeID() const {
        return *_nodeIt;
    }

    GetNodePropertiesIteratorWithNull<T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive* operator*() const {
        return get();
    }

protected:
    PropertyTypeID _propTypeID;
    const T::Primitive* _prop {nullptr};
    const ColumnIDs* _inputNodeIDs {nullptr};
    ColumnIDs::ConstIterator _nodeIt;

    void init();
};

template <SupportedType T>
class GetNodePropertiesWithNullChunkWriter : public GetNodePropertiesIteratorWithNull<T> {
public:
    GetNodePropertiesWithNullChunkWriter(const GraphView& view,
                                        PropertyTypeID propTypeID,
                                        const ColumnIDs* inputNodeIDs);

    void fill(size_t maxCount);

    void setOutput(ColumnOptVector<typename T::Primitive>* output) { _output = output; }

private:
    ColumnOptVector<typename T::Primitive>* _output {nullptr};
};

}
