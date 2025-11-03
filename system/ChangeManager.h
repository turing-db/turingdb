#pragma once

#include <unordered_map>

#include "RWSpinLock.h"
#include "versioning/ChangeResult.h"
#include "versioning/ChangeID.h"
#include "versioning/Change.h"

namespace db {

class Graph;

class JobSystem;

class ChangeManager {
public:
    struct GraphChangeKey {
        std::uintptr_t _graphAddr;
        ChangeID _changeID;

        struct Hasher {
            size_t operator()(const GraphChangeKey& pair) const {
                return std::hash<std::uintptr_t> {}(pair._graphAddr)
                     ^ std::hash<ChangeID::ValueType> {}(pair._changeID.get());
            }
        };

        struct Predicate {
            bool operator()(const GraphChangeKey& a, const GraphChangeKey& b) const {
                return a._graphAddr == b._graphAddr && a._changeID == b._changeID;
            }
        };
    };

    struct GraphChangeValue {
        Graph* _graph {nullptr};
        std::unique_ptr<Change> _change;
    };

    ChangeManager();
    ~ChangeManager();

    ChangeManager(const ChangeManager&) = delete;
    ChangeManager(ChangeManager&&) = delete;
    ChangeManager& operator=(const ChangeManager&) = delete;
    ChangeManager& operator=(ChangeManager&&) = delete;

    Change* storeChange(Graph* graph, std::unique_ptr<Change> change);
    ChangeResult<Change*> getChange(const Graph* graph, ChangeID changeID);
    ChangeResult<void> submitChange(const Graph* graph, ChangeAccessor& access, JobSystem&);
    ChangeResult<void> deleteChange(const Graph* graph, ChangeAccessor& access, ChangeID changeID);

    void listChanges(std::vector<const Change*>& list, const Graph* graph) const;

private:
    mutable RWSpinLock _changesLock;

    std::unordered_map<GraphChangeKey,
                       GraphChangeValue,
                       GraphChangeKey::Hasher,
                       GraphChangeKey::Predicate>
        _changes;
};

}
