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
    // A part's property column and the ID range it covers, which bounds it only when sorted.
    struct PartContainer {
        const TypedPropertyContainer<T>* _container {nullptr};
        EntityID _first;
        EntityID _last;
        bool _sorted {false};
    };

    PropertyTypeID _propTypeID;
    Primitive _value {};
    LabelSetHandle _labelset;
    const TypedPropertyContainer<T>* _container {nullptr};
    std::vector<PartContainer> _partContainers;
    std::vector<PartContainer> _newerContainers;
    std::vector<PropertyRange> _wholeContainer;
    LabelSetPropertyIndexer::MatchIterator _labelsetIt;
    std::vector<PropertyRange>::const_iterator _rangeIt;
    std::vector<PropertyRange>::const_iterator _rangeEnd;
    std::span<const Primitive> _values;
    std::span<const EntityID> _ids;
    size_t _offset {0};
    bool _denseIDs {false};
    ColumnNodeIDs* _nodeIDs {nullptr};

    TombstoneFilter _filter;

    void collectPartContainers();
    bool startPart();
    void collectNewerContainers(size_t partIndex);
    bool nextSliceInPart();
    void seekSliceAcrossParts();
    void nextSlice();
    size_t scanSlice(size_t rows, NodeID* hits) const;
    size_t dropOverriddenHits(NodeID* hits, size_t count) const;
    void filterTombstones();
};

static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::UInt64>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Int64>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Double>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::String>>);
static_assert(NodeIDsChunkWriter<ScanNodesByPropertyValueChunkWriter<types::Bool>>);

}
