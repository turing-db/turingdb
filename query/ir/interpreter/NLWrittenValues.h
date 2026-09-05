#pragma once

#include <stddef.h>
#include <stdint.h>

#include <deque>
#include <unordered_map>

#include "ID.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

// What this change has written, as a read later in the same query sees it. The graph a
// fetch reads holds the values from before the change: an update to a committed entity
// only lands there at commit, so the fetch has to be told about it here.
class NLWrittenValues {
public:
    using Value = CommitWriteBuffer::SupportedTypeVariant;

    NLWrittenValues();
    ~NLWrittenValues();

    // Takes in every update the buffer has gathered since the last call. It only ever
    // appends while a program runs, so what is indexed stays indexed.
    void indexUpdates(const CommitWriteBuffer* writeBuffer);

    bool hasUpdates() const { return !_nodeUpdates.empty() || !_edgeUpdates.empty(); }

    const Value* findNodeUpdate(NodeID node, PropertyTypeID property) const;
    const Value* findEdgeUpdate(EdgeID edge, PropertyTypeID property) const;

    // A copy of a value a fetch is about to hand to a column that only borrows it - a
    // string or an embedding. The change rewrites its own values as the query runs, so
    // the column would otherwise come to point at bytes a later row has freed.
    const Value& retain(const Value& value);

private:
    struct Key {
        uint64_t _entity {0};
        uint64_t _property {0};

        bool operator==(const Key& other) const {
            return _entity == other._entity && _property == other._property;
        }
    };

    struct KeyHash {
        size_t operator()(const Key& key) const {
            return (key._entity * 1099511628211ull) ^ key._property;
        }
    };

    using UpdateIndex = std::unordered_map<Key, size_t, KeyHash>;

    const CommitWriteBuffer* _writeBuffer {nullptr};

    // Each key's row in the buffer's own update list rather than the value itself, since
    // the list moves what it holds as it grows
    UpdateIndex _nodeUpdates;
    UpdateIndex _edgeUpdates;

    size_t _indexedNodeUpdates {0};
    size_t _indexedEdgeUpdates {0};

    std::deque<Value> _retained;
};

}
