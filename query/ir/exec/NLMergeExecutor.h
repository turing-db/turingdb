#pragma once

#include <stddef.h>

#include "versioning/CommitWriteBuffer.h"

#include "NLMergeWorkingSet.h"
#include "NLProgram.h"

namespace db {

class GraphView;
class NLExecutionContext;

// Runs one nl.merge over the rows its input chunks hold: each looked up against the
// graph and against the entities this query already wrote, then written where the
// pattern is missing.
class NLMergeExecutor {
public:
    NLMergeExecutor(NLExecutionContext* context, NLMergeData* data);
    ~NLMergeExecutor();

    void run();

private:
    NLExecutionContext* _context {nullptr};
    NLMergeData* _data {nullptr};
    NLMergeWorkingSet* _work {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};
    const GraphView* _view {nullptr};

    // A ref this step holds names a pending entity by its write-buffer offset, while the
    // columns it reads and fills name one by the ID it will commit as: one past the last
    // the graph holds, plus that offset
    size_t _firstPendingNodeID {0};
    size_t _firstPendingEdgeID {0};

    void extractProperties(size_t rowCount);
    void clearResults();

    void buildNodeIndex(NLMergeNodeIndex* index);
    void collectCandidates(size_t row);
    void matchRow(size_t row);
    void buildHopKeys(const NLMergeData::Hop& hop, size_t row);
    void extendHop(size_t hopIndex);

    void collectGraphExtensions(const NLMergeData::Hop& hop, size_t hopIndex);
    void collectDirectedExtensions(const NLMergeData::Hop& hop, size_t hopIndex, bool outgoing);
    void dropExtensionsWithOtherProperties(const NLMergeData::Hop& hop);
    void groupExtensionsBySource();

    void emitRow(const NLMergePartialMatch& match, size_t row, bool created);
    void writeRow(size_t row);

    NLMergeRef writeNode(const NLMergeData::Node& node, size_t nodeIndex, size_t row);
    NLMergeRef writeEdge(const NLMergeData::Hop& hop,
                         size_t hopIndex,
                         size_t row,
                         const NLMergeRef& source,
                         const NLMergeRef& target);

    static CommitWriteBuffer::ExistingOrPendingNode asWriteBufferNode(const NLMergeRef& ref);

    void gatherCarriedColumns();
};

}
