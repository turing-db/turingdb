#pragma once

#include "Iterator.h"

#include "ChunkWriter.h"
#include "columns/ColumnVector.h"
#include "iterators/PartIterator.h"
#include "properties/PropertyManager.h"
#include "types/PropertyType.h"

namespace db {

template <SupportedType T>
class ScanNodePropertiesByLabelIterator : public Iterator {
public:
    ScanNodePropertiesByLabelIterator() = default;
    ScanNodePropertiesByLabelIterator(const ScanNodePropertiesByLabelIterator&) = default;
    ScanNodePropertiesByLabelIterator(ScanNodePropertiesByLabelIterator&&) = default;
    ScanNodePropertiesByLabelIterator& operator=(const ScanNodePropertiesByLabelIterator&) = default;
    ScanNodePropertiesByLabelIterator& operator=(ScanNodePropertiesByLabelIterator&&) = default;

    ScanNodePropertiesByLabelIterator(const GraphView& view,
                                      PropertyTypeID propTypeID,
                                      const LabelSet* labelset);

    ~ScanNodePropertiesByLabelIterator() override;

    void init();
    void next() override;
    void reset();

    const T::Primitive& get() const {
        return *_propIt;
    }

    EntityID getCurrentNodeID() const {
        return *_currentIDIt;
    }

    ScanNodePropertiesByLabelIterator<T>& operator++() {
        next();
        return *this;
    }

    const T::Primitive& operator*() const {
        return get();
    }

protected:
    PropertyTypeID _propTypeID;
    const LabelSet* _labelset {nullptr};
    using LabelSetIterator = LabelSetPropertyIndexer::MatchIterator;
    LabelSetIterator _labelsetIt;
    std::vector<PropertyRange>::const_iterator _rangeIt;
    std::span<const typename T::Primitive> _props {};
    std::span<const typename T::Primitive>::iterator _propIt {};
    std::span<const EntityID>::iterator _currentIDIt;

    bool nextDatapart();
    void newPropertySpan();
    void nextValid();
};

template <SupportedType T>
class ScanNodePropertiesByLabelChunkWriter : public ScanNodePropertiesByLabelIterator<T> {
public:
    using Type = T;
    using Primitive = T::Primitive;

    ScanNodePropertiesByLabelChunkWriter();
    ScanNodePropertiesByLabelChunkWriter(const GraphView& view,
                                         PropertyTypeID propTypeID,
                                         const LabelSet* labelset);

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
struct ScanNodePropertiesByLabelRange {
    GraphView _view;
    PropertyTypeID _propTypeID {0};
    const LabelSet* _labelset {nullptr};

    ScanNodePropertiesByLabelIterator<T> begin() const { return {_view, _propTypeID, _labelset}; }
    DataPartIterator end() const { return PartIterator(_view).getEndIterator(); }
    ScanNodePropertiesByLabelChunkWriter<T> chunkWriter() const { return {_view, _propTypeID, _labelset}; }
};


static_assert(PropertiesChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::UInt64>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Int64>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Double>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::String>>);
static_assert(PropertiesChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Bool>>);

static_assert(NodeIDsChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::UInt64>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Int64>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Double>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::String>>);
static_assert(NodeIDsChunkWriter<ScanNodePropertiesByLabelChunkWriter<types::Bool>>);

}
