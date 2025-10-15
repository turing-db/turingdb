#pragma once

#include "versioning/WriteSet.h"
#include "ID.h"

namespace db {

class CommitWriteBuffer;

/**
 * @brief Stores information as to the modifications (updates/deletes) that occured in
 * this commit. That is, the updates and deletes which, when applied to the last commit,
 * resulted in the state of this commit.
 * @detail The first commit always contains an empty journal.
 */
class CommitJournal {
public:
    /**
     * @brief Creates a new journal with an empty write set
     */
    [[nodiscard]] static std::unique_ptr<CommitJournal> emptyJournal();

    void clear();
    bool empty();

    void addWrittenNode(NodeID node);
    void addWrittenEdge(EdgeID edge);

    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, NodeID>
    void addWrittenNodes(const Range& nodes);

    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, EdgeID>
    void addWrittenEdges(const Range& edges);

    auto& nodeWriteSet() { return _nodeWriteSet; }
    auto& edgeWriteSet() { return _edgeWriteSet; }

    void finalise();

private:
    bool _initialised {false};

    WriteSet<NodeID> _nodeWriteSet;
    WriteSet<EdgeID> _edgeWriteSet;

    CommitJournal() = default;
};

template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, NodeID>
void CommitJournal::addWrittenNodes(const Range& nodes) {
    _nodeWriteSet.insert(nodes);
}

template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, EdgeID>
void CommitJournal::addWrittenEdges(const Range& edges) {
    _edgeWriteSet.insert(edges);
}

}
