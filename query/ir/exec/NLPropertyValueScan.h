#pragma once

#include <stddef.h>
#include <stdint.h>

#include <unordered_map>
#include <vector>

#include "columns/ColumnIDs.h"
#include "iterators/ScanNodesByPropertyValueIterator.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"

#include "ID.h"

namespace db {

class NLExecutionContext;

// The step of a scan by property value that reads the graph's nodes. The graph holds the
// values from before the change until the commit, so a node the change updated away from
// the value is dropped from what the graph finds, and a node it updated to the value is
// filled once the graph is drained.
template <SupportedType T>
class NLPropertyValueScan {
public:
    using Primitive = typename T::Primitive;

    NLPropertyValueScan(NLExecutionContext* context,
                        ColumnNodeIDs* nodeIDs,
                        PropertyTypeID propertyType,
                        const Primitive& value,
                        const LabelSetHandle& labelset);
    ~NLPropertyValueScan();

    bool isValid() const;

    void fill(size_t maxCount);

private:
    enum class UpdatedNode {
        HoldsValue,
        LostValue,
        FoundInGraph,
    };

    ScanNodesByPropertyValueChunkWriter<T> _graphNodes;
    ColumnNodeIDs* _nodeIDs {nullptr};

    // The updates as they stood when the scan started: a SET in the scan's own body writes
    // past them, and what the scan reads is what the query wrote before it ran.
    std::unordered_map<uint64_t, UpdatedNode> _updatedNodes;

    std::vector<NodeID> _gainedNodes;
    size_t _nextGainedNode {0};

    void dropNodesThatLostTheValue();
};

}
