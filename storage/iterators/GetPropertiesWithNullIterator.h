#pragma once

#include "Iterator.h"
#include "PartIterator.h"

#include "columns/ColumnOptVector.h"
#include "metadata/SupportedType.h"

namespace db {

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

    std::optional<typename T::Primitive> get() const {
        if (_prop != nullptr) {
            return *_prop;
        }

        return std::nullopt;
    }

    ID getCurrentID() const {
        return *_entityIt;
    }

    GetPropertiesIteratorWithNull& operator++() {
        next();
        return *this;
    }

    std::optional<typename T::Primitive> operator*() const {
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
struct GetPropertiesWithNullRange {
    using ColumnIDs = ColumnVector<ID>;

    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const ColumnIDs* _inputIDs {nullptr};

    GetPropertiesIteratorWithNull<ID, T> begin() const { return {_view, _propTypeID, _inputIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
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
using GetNodePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<NodeID, T>;

template <SupportedType T>
using GetNodePropertiesWithNullRange = GetPropertiesWithNullRange<NodeID, T>;

template <SupportedType T>
using GetNodePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<NodeID, T>;

template <SupportedType T>
using GetEdgePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<EdgeID, T>;

template <SupportedType T>
using GetEdgePropertiesWithNullRange = GetPropertiesWithNullRange<EdgeID, T>;

template <SupportedType T>
using GetEdgePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<EdgeID, T>;

}
