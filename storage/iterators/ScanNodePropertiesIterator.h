#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "ChunkWriter.h"
#include "columns/ColumnVector.h"
#include "types/PropertyType.h"

namespace db {

template <SupportedType T>
class ScanNodePropertiesIterator : public Iterator {
public:
    ScanNodePropertiesIterator() = default;
    ScanNodePropertiesIterator(const ScanNodePropertiesIterator&) = default;
    ScanNodePropertiesIterator(ScanNodePropertiesIterator&&) = default;
    ScanNodePropertiesIterator& operator=(const ScanNodePropertiesIterator&) = default;
    ScanNodePropertiesIterator& operator=(ScanNodePropertiesIterator&&) = default;

    ScanNodePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID);
    ~ScanNodePropertiesIterator() override;

    void reset();

    void next() override;

    const T::Primitive& get() const;

    ScanNodePropertiesIterator<T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

    EntityID getCurrentNodeID() const {
        return *_currentID;
    }

protected:
    using SpanUnion = union {
        std::span<const typename T::Primitive> mandatory;
        typename T::OptionalSpan optional;
    };

    using IteratorUnion = union {
        std::span<const typename T::Primitive>::iterator mandatory;
        typename T::OptionalSpan::iterator optional;
    };

    PropertyTypeID _propTypeID;
    std::span<const typename T::Primitive> _props {};
    std::span<const typename T::Primitive>::iterator _propIt {};
    std::span<const EntityID>::iterator _currentID;

    void nextValid();
    bool nextDatapart();
    void newPropertySpan();
    void init();
};

template <SupportedType T>
class ScanNodePropertiesChunkWriter : public ScanNodePropertiesIterator<T> {
public:
    using Type = T;
    using Primitive = T::Primitive;

    ScanNodePropertiesChunkWriter();
    ScanNodePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID);

    void fill(size_t maxCount);

    void setProperties(ColumnVector<Primitive>* properties) {
        _properties = properties;
    }

    void setNodeIDs(ColumnVector<EntityID>* nodeIDs) {
        _nodeIDs = nodeIDs;
    }

private:
    ColumnVector<Primitive>* _properties {nullptr};
    ColumnVector<EntityID>* _nodeIDs {nullptr};
};

template <SupportedType T>
struct ScanNodePropertiesRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};

    ScanNodePropertiesIterator<T> begin() const { return {_view, _propTypeID}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodePropertiesChunkWriter<T> chunkWriter() const { return {_view, _propTypeID}; }
};

static_assert(PropertiesChunkWriter<ScanNodePropertiesChunkWriter<types::UInt64>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesChunkWriter<types::Int64>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesChunkWriter<types::Double>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesChunkWriter<types::String>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesChunkWriter<types::Bool>>);

static_assert(NodeIDsChunkWriter<ScanNodePropertiesChunkWriter<types::UInt64>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesChunkWriter<types::Int64>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesChunkWriter<types::Double>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesChunkWriter<types::String>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesChunkWriter<types::Bool>>);

}
