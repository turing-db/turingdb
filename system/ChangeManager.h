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
    struct GraphChangePair {
        const Graph* _graph;
        ChangeID _changeID;

        struct Hasher {
            size_t operator()(const GraphChangePair& pair) const {
                return std::hash<const Graph*> {}(pair._graph)
                     ^ std::hash<ChangeID::ValueType> {}(pair._changeID.get());
            }
        };

        struct Predicate {
            bool operator()(const GraphChangePair& a, const GraphChangePair& b) const {
                return a._graph == b._graph && a._changeID == b._changeID;
            }
        };
    };

    ChangeManager();
    ~ChangeManager();

    ChangeManager(const ChangeManager&) = delete;
    ChangeManager(ChangeManager&&) = delete;
    ChangeManager& operator=(const ChangeManager&) = delete;
    ChangeManager& operator=(ChangeManager&&) = delete;

    Change* storeChange(const Graph* graph, std::unique_ptr<Change> change);
    ChangeResult<Change*> getChange(const Graph* graph, ChangeID changeID);
    ChangeResult<void> acceptChange(ChangeAccessor& access, JobSystem&);
    ChangeResult<void> deleteChange(ChangeAccessor& access, ChangeID changeID);

    void listChanges(std::vector<const Change*>& list, const Graph* graph) const;

private:
    mutable RWSpinLock _changesLock;

    std::unordered_map<GraphChangePair,
                       std::unique_ptr<Change>,
                       GraphChangePair::Hasher,
                       GraphChangePair::Predicate>
        _changes;
};

}
