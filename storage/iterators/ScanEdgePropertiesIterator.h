#pragma once

#include "Iterator.h"

#include "PartIterator.h"
#include "ChunkWriter.h"
#include "TombstoneFilter.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

namespace db {

template <SupportedType T>
class ScanEdgePropertiesIterator : public Iterator {
public:
    ScanEdgePropertiesIterator() = default;
    ScanEdgePropertiesIterator(const ScanEdgePropertiesIterator&) = default;
    ScanEdgePropertiesIterator(ScanEdgePropertiesIterator&&) noexcept = default;
    ScanEdgePropertiesIterator& operator=(const ScanEdgePropertiesIterator&) = default;
    ScanEdgePropertiesIterator& operator=(ScanEdgePropertiesIterator&&) noexcept = default;

    ScanEdgePropertiesIterator(const GraphView& view, PropertyTypeID propTypeID);
    ~ScanEdgePropertiesIterator() override;

    void next() override;

    const T::Primitive& get() const;

    ScanEdgePropertiesIterator<T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

    EdgeID getCurrentEdgeID() const;

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
    std::span<const typename T::Primitive>::iterator _propIt;
    std::span<const EntityID>::iterator _currentID;

    bool nextDatapart();
    void newPropertySpan();
    void nextValid();
};

template <SupportedType T>
class ScanEdgePropertiesChunkWriter : public ScanEdgePropertiesIterator<T> {
public:
    using Type = T;
    using Primitive = T::Primitive;

    ScanEdgePropertiesChunkWriter() = default;
    ScanEdgePropertiesChunkWriter(const GraphView& view, PropertyTypeID propTypeID);

    void fill(size_t maxCount);

    void setProperties(ColumnVector<Primitive>* properties) { _properties = properties; }
    void setEdgeIDs(ColumnEdgeIDs* edgeIDs) { _edgeIDs = edgeIDs; }

private:
    ColumnEdgeIDs* _edgeIDs {nullptr};
    ColumnVector<Primitive>* _properties {nullptr};

    TombstoneFilter _filter;

    void filterTombstones();
};

template <SupportedType T>
struct ScanEdgePropertiesRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};

    ScanEdgePropertiesIterator<T> begin() const { return {_view, _propTypeID}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanEdgePropertiesChunkWriter<T> chunkWriter() const { return {_view, _propTypeID}; }
};

static_assert(PropertiesChunkWriter<ScanEdgePropertiesChunkWriter<types::UInt64>>);
static_assert(PropertiesChunkWriter<ScanEdgePropertiesChunkWriter<types::Int64>>);
static_assert(PropertiesChunkWriter<ScanEdgePropertiesChunkWriter<types::Double>>);
static_assert(PropertiesChunkWriter<ScanEdgePropertiesChunkWriter<types::String>>);
static_assert(PropertiesChunkWriter<ScanEdgePropertiesChunkWriter<types::Bool>>);

static_assert(EdgeIDsChunkWriter<ScanEdgePropertiesChunkWriter<types::UInt64>>);
static_assert(EdgeIDsChunkWriter<ScanEdgePropertiesChunkWriter<types::Int64>>);
static_assert(EdgeIDsChunkWriter<ScanEdgePropertiesChunkWriter<types::Double>>);
static_assert(EdgeIDsChunkWriter<ScanEdgePropertiesChunkWriter<types::String>>);
static_assert(EdgeIDsChunkWriter<ScanEdgePropertiesChunkWriter<types::Bool>>);

}

