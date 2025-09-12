#pragma once

#include "CreateTarget.h"
#include "ExecutionContext.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

class EntityPattern;
class PathPattern;

class WriteStep {
public:
    struct Tag {};  

    explicit WriteStep(const CreateTargets* targets)
        : _targets(targets)
    {
    }

    WriteStep(WriteStep&& other) = default;

    ~WriteStep() = default;

    void prepare(ExecutionContext* ctxt);

    void reset() {}

    bool isFinished() const { return true; }

    void execute();

    void describe(std::string& descr) const;

private:
    using UntypedProperties = std::vector<CommitWriteBuffer::UntypedProperty>;
    using ContingentNode = CommitWriteBuffer::ContingentNode;

    const CreateTargets* _targets {nullptr};
    CommitWriteBuffer* _writeBuffer {nullptr};

    /**
     * @brief Mapping from variable names to offsets in @ref _writeBuffer 's @ref
     * _pendingNodes vector
     * @detail Allows nodes which may be referred to by edges in the same or a different
     * CreateTarget to be correctly referred to when creating PendingEdges in the
     * CommitWriteBuffer
     */
    std::unordered_map<std::string, CommitWriteBuffer::PendingNodeOffset> _varOffsetMap;

    /**
     * @brief If the node specified by @param nodePattern already exists, returns its
     * NodeID; if the node specified by @param nodePattern has already been written in
     * this WriteStep, returns the offset in @ref _writeBuffer 's @ref _pendingNodes
     * vector where it may be found; otherwise writes the node and returns its offset.
     * @detail Calls @ref writeNode if a node need be written.
     */
    ContingentNode getOrWriteNode(const EntityPattern* nodePattern);

    /**
     * @brief Writes a node to @ref _writeBuffer according to @param nodePattern to @ref
     * _writeBuffer, and returns the offset at which it can be found.
     */
    CommitWriteBuffer::PendingNodeOffset writeNode(const EntityPattern* nodePattern);

    void writeEdge(const ContingentNode src, const ContingentNode tgt,
                   const EntityPattern* edgePattern);

    void writeEdges(const PathPattern* pathPattern);
};
}
