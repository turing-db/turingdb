#pragma once

#include <vector>

#include "Iterator.h"

#include "ChunkWriter.h"
#include "TombstoneFilter.h"
#include "columns/ColumnIDs.h"
#include "indexers/PropertyIndexer.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"

namespace db {

template <SupportedType T>
class TypedPropertyContainer;

template <SupportedType T>
class ScanNodesByPropertyValueChunkWriter : public Iterator {
public:
    using Primitive = T::Primitive;

    ScanNodesByPropertyValueChunkWriter() = delete;
    ScanNodesByPropertyValueChunkWriter(const GraphView& view, PropertyTypeID propTypeID, const Primitive& value);
    ScanNodesByPropertyValueChunkWriter(const GraphView& view,
                                        PropertyTypeID propTypeID,
                                        const Primitive& value,
                                        const LabelSetHandle& labelset);
    ~ScanNodesByPropertyValueChunkWriter() override;

    void next() override;

    void setNodeIDs(ColumnNodeIDs* nodeIDs) {
        _nodeIDs = nodeIDs;
    }

    void fill(size_t maxCount);

private:
    PropertyTypeID _propTypeID;
    Primitive _value {};
    LabelSetHandle _labelset;
    const TypedPropertyContainer<T>* _container {nullptr};
    std::vector<PropertyRange> _wholeContainer;
    LabelSetPropertyIndexer::MatchIterator _labelsetIt;
    std::vector<PropertyRange>::const_iterator _rangeIt;
    std::vector<PropertyRange>::const_iterator _rangeEnd;
    std::span<const Primitive> _values;
    std::span<const EntityID> _ids;
    size_t _offset {0};
    ColumnNodeIDs* _nodeIDs {nullptr};

    TombstoneFilter _filter;

    bool startPart();
    bool nextSliceInPart();
    void seekSliceAcrossParts();
    void nextSlice();
    size_t dropOverriddenHits(NodeID* hits, size_t count) const;
    void filterTombstones();
};

static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::UInt64>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Int64>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Double>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::String>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Bool>>);

}
