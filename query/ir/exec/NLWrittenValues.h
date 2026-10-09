#pragma once

#include <stddef.h>
#include <stdint.h>

#include <optional>
#include <unordered_map>
#include <vector>

#include "ID.h"
#include "NLValueStore.h"
#include "metadata/PropertyType.h"
#include "metadata/SupportedType.h"
#include "versioning/CommitWriteBuffer.h"

namespace db {

// What this change has written, as a read later in the same query sees it. The graph a
// fetch reads holds the values from before the change: an update to a committed entity
// only lands there at commit, so the fetch has to be told about it here.
class NLWrittenValues {
public:
    using Value = CommitWriteBuffer::SupportedTypeVariant;

    struct PendingNodeUpdate {
        size_t _offset {0};
        PropertyTypeID _property;
    };

    NLWrittenValues();
    ~NLWrittenValues();

    // Takes in every update the buffer has gathered since the last call. It only ever
    // appends while a program runs, so what is indexed stays indexed.
    void indexUpdates(const CommitWriteBuffer* writeBuffer);

    bool hasUpdates() const { return !_nodeUpdates.empty() || !_edgeUpdates.empty(); }

    const Value* findNodeUpdate(NodeID node, PropertyTypeID property) const;
    const Value* findEdgeUpdate(EdgeID edge, PropertyTypeID property) const;

    template <TypedInternalID IDT>
    const Value* findUpdate(IDT entity, PropertyTypeID property) const;

    // One value this change wrote, as a column of the property holds it. The value is held
    // as whatever type the row's own column carried, so it is converted to the type the
    // schema holds the property as - which is the column's element type. A string,
    // embedding, list or map is copied into @param values, since the change rewrites its
    // own values as the query runs and the column would otherwise point at freed bytes.
    template <SupportedType T>
    std::optional<typename T::Primitive> read(const Value& value, NLValueStore& values);

    // A node this query wrote is updated in place in the write buffer, which keeps no
    // trace of it: a reader that took the node in before finds here what changed since
    void addPendingNodeUpdate(size_t offset, PropertyTypeID property);
    const std::vector<PendingNodeUpdate>& pendingNodeUpdates() const { return _pendingNodeUpdates; }


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

    std::vector<PendingNodeUpdate> _pendingNodeUpdates;
};

}
