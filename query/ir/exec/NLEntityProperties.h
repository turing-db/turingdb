#pragma once

#include <stddef.h>

#include <string_view>
#include <vector>

#include "ID.h"
#include "map/MapBuffer.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class GraphView;
class MetadataBuilder;
class NLExecutionContext;
class NLWrittenValues;

// The properties of a node or an edge as a read in this query sees them: the newest value
// the graph holds for each, overlaid with what the change wrote. A property a newer write
// nulled out is not one the entity holds.
class NLEntityProperties {
public:
    using Entry = MapBuffer<>::MapKeyValuePair;

    explicit NLEntityProperties(NLExecutionContext* context);
    ~NLEntityProperties();

    template <TypedInternalID IDT>
    void readKeys(IDT entity, std::vector<std::string_view>& keys);

    template <TypedInternalID IDT>
    void readEntries(IDT entity, std::vector<Entry>& entries);

private:
    struct Property {
        PropertyTypeID _id;
        std::string_view _name;
        MapBuffer<>::MapItemVariant _value;
    };

    const GraphView* _view {nullptr};
    const CommitWriteBuffer* _writeBuffer {nullptr};
    NLWrittenValues* _written {nullptr};
    const MetadataBuilder* _metadataBuilder {nullptr};
    size_t _committedNodeCount {0};
    size_t _committedEdgeCount {0};
    bool _readsValues {false};
    std::vector<Property> _properties;
    std::vector<PropertyTypeID> _decided;
    std::vector<bool> _isDecided;

    template <TypedInternalID IDT>
    void collect(IDT entity);

    template <TypedInternalID IDT>
    void collectCommitted(IDT entity);

    template <TypedInternalID IDT>
    void collectPending(IDT entity);

    template <TypedInternalID IDT>
    void collectUpdates(IDT entity);

    template <TypedInternalID IDT>
    bool isPending(IDT entity) const;

    void addWritten(PropertyTypeID id, const CommitWriteBuffer::SupportedTypeVariant& value);
    void add(PropertyTypeID id, const MapBuffer<>::MapItemVariant& value);
    std::string_view getName(PropertyTypeID id) const;
    ValueType getValueType(PropertyTypeID id) const;
};

}
