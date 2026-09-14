#pragma once

#include <stddef.h>

#include <optional>

#include "columns/ColumnIDs.h"
#include "metadata/LabelSetHandle.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

#include "ID.h"

namespace db {

class NLExecutionContext;

// The step of a node scan that reads the nodes this change has written and not committed.
// The graph holds none of them until the commit, so a scan reading only the graph would
// miss every node the query itself wrote. It fills the same node chunk the committed step
// fills and runs once that one is drained, so the rows of a scan are the graph's nodes then
// this change's.
class NLPendingNodeScan {
public:
    NLPendingNodeScan(NLExecutionContext* context, ColumnNodeIDs* nodeIDs);
    ~NLPendingNodeScan();

    // The label set a node must carry at least, and the property value it must hold, for
    // the scan to keep it. The label set is borrowed, so it must outlive the scan.
    void setLabelSet(const LabelSet& labelset);
    void setProperty(PropertyTypeID propertyType, const CommitWriteBuffer::SupportedTypeVariant& value);

    bool isValid() const { return _node < _pendingNodeCount; }

    void fill(size_t maxCount);

private:
    const CommitWriteBuffer* _writeBuffer {nullptr};
    ColumnNodeIDs* _nodeIDs {nullptr};

    size_t _firstPendingNodeID {0};

    // What the buffer held when the scan started. A create in the scan's own body writes
    // past this, and what the scan reads is what the query wrote before it ran.
    size_t _pendingNodeCount {0};

    // Starts at the query's own first pending node: what an earlier statement of the change
    // staged is read once it commits.
    size_t _node {0};

    LabelSetHandle _labelset;
    PropertyTypeID _propertyType;
    std::optional<CommitWriteBuffer::SupportedTypeVariant> _value;

    bool matches(const CommitWriteBuffer::PendingNode& node) const;
};

}
