#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "types/SupportedType.h"

namespace db {

enum class PropertyIteratorClass {
    NODE,
    EDGE
};

template <PropertyIteratorClass IteratorClass, SupportedType T>
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

    GetPropertiesIterator<IteratorClass, T>& operator++() {
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

template <PropertyIteratorClass IteratorClass, SupportedType T>
struct GetPropertiesRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const ColumnIDs* _inputEntityIDs {nullptr};

    GetPropertiesIterator<IteratorClass, T> begin() const { return {_view, _propTypeID, _inputEntityIDs}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
};

template <PropertyIteratorClass IteratorClass, SupportedType T>
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

    GetPropertiesIteratorWithNull<IteratorClass, T>& operator++() {
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

template <PropertyIteratorClass IteratorClass, SupportedType T>
class GetPropertiesWithNullChunkWriter : public GetPropertiesIteratorWithNull<IteratorClass, T> {
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
using GetNodePropertiesIterator = GetPropertiesIterator<PropertyIteratorClass::NODE, T>;
template <SupportedType T>
using GetNodePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<PropertyIteratorClass::NODE, T>;
template <SupportedType T>
using GetNodePropertiesRange = GetPropertiesRange<PropertyIteratorClass::NODE, T>;
template <SupportedType T>
using GetNodePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<PropertyIteratorClass::NODE, T>;

template <SupportedType T>
using GetEdgePropertiesIterator = GetPropertiesIterator<PropertyIteratorClass::EDGE, T>;
template <SupportedType T>
using GetEdgePropertiesIteratorWithNull = GetPropertiesIteratorWithNull<PropertyIteratorClass::EDGE, T>;
template <SupportedType T>
using GetEdgePropertiesRange = GetPropertiesRange<PropertyIteratorClass::EDGE, T>;
template <SupportedType T>
using GetEdgePropertiesWithNullChunkWriter = GetPropertiesWithNullChunkWriter<PropertyIteratorClass::EDGE, T>;

}
