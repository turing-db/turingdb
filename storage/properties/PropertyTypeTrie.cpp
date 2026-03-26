#include "PropertyTypeTrie.h"

#include "BioAssert.h"
#include "Profiler.h"
#include "FatalException.h"

using namespace db;

PropertyTypeTrie::PropertyTypeTrie() = default;

PropertyTypeTrie::~PropertyTypeTrie() = default;

void PropertyTypeTrie::build(const std::unordered_map<PropertyTypeID, std::span<const EntityID>>& typesToEntities,
                             EntityID firstCoreEntityID,
                             size_t coreEntityCount) {
    const Profile profile("PropertyTypeTrie::build");

    _firstCoreEntityID = firstCoreEntityID;
    _coreEntityCount = coreEntityCount;

    // Step 1. Sort PropertyTypeIDs.
    //         Sorted order guarantees that two entities with the same set
    //         follow the exact same trie path and land on the same leaf node.
    std::vector<PropertyTypeID> sortedTypes;
    {
        const Profile profile("PropertyTypeTrie::build <sort types>");
        sortedTypes.reserve(typesToEntities.size());

        for (const auto& [ptId, entities] : typesToEntities) {
            sortedTypes.push_back(ptId);
        }

        std::sort(sortedTypes.begin(), sortedTypes.end());
    }

    // Step 2. Create the root node (empty PropertyTypeSet) and register every
    //         entity that has at least one property type, mapping it to the root (index 0).
    //         Patch entities (id < firstCoreEntityID) go into a sorted map;
    //         core entities go into a vector indexed by (id - firstCoreEntityID).
    _nodes.clear();
    _nodes.emplace_back(); // root is always index 0
    _coreEntityToNode.assign(coreEntityCount, INVALID_NODE);

    for (const auto& [ptId, entities] : typesToEntities) {
        for (const EntityID e : entities) {
            if (e < firstCoreEntityID) {
                _patchEntityToNode.emplace(e, 0);
            } else {
                const size_t idx = (e - firstCoreEntityID).getValue();
                if (idx < coreEntityCount && _coreEntityToNode[idx] == INVALID_NODE) {
                    _coreEntityToNode[idx] = 0;
                }
            }
        }
    }

    // Step 3. For each PT in sorted order, merge-walk patch entities then core
    //         entities with the PT's span, routing each entity to the appropriate
    //         child. Children are created lazily on first use.
    //
    //         Because patch IDs < firstCoreEntityID <= core IDs, and spans are
    //         sorted, a single spanIt carried from the patch loop into the core
    //         loop produces a correct sorted merge.
    for (const PropertyTypeID ptId : sortedTypes) {
        const std::span<const EntityID> span = typesToEntities.at(ptId);
        auto spanIt = span.begin();

        for (auto& [e, nodeIdx] : _patchEntityToNode) {
            while (spanIt != span.end() && *spanIt < e) {
                ++spanIt;
            }

            const uint16_t curIdx = nodeIdx;
            const bool hasIt = (spanIt != span.end() && *spanIt == e);

            if (hasIt) {
                if (_nodes[curIdx]._hasPt == INVALID_NODE) {
                    // Copy before allocNode(): push_back may reallocate _nodes.
                    const PropertyTypeSet ptsCopy = _nodes[curIdx]._propertyTypeSet;
                    const uint16_t newIdx = allocNode();
                    _nodes[newIdx]._propertyTypeSet = ptsCopy;
                    _nodes[newIdx]._propertyTypeSet.add(ptId); // sorted: ptId > all previous
                    _nodes[curIdx]._hasPt = newIdx;
                }

                nodeIdx = _nodes[curIdx]._hasPt;
                ++spanIt;
            } else {
                if (_nodes[curIdx]._lacksPt == INVALID_NODE) {
                    const PropertyTypeSet ptsCopy = _nodes[curIdx]._propertyTypeSet;
                    const uint16_t newIdx = allocNode();
                    _nodes[newIdx]._propertyTypeSet = ptsCopy;
                    _nodes[curIdx]._lacksPt = newIdx;
                }

                nodeIdx = _nodes[curIdx]._lacksPt;
            }
        }

        for (size_t idx = 0; idx < coreEntityCount; ++idx) {
            if (_coreEntityToNode[idx] == INVALID_NODE) {
                continue;
            }

            const EntityID e = firstCoreEntityID + idx;

            while (spanIt != span.end() && *spanIt < e) {
                ++spanIt;
            }

            const uint16_t curIdx = _coreEntityToNode[idx];
            const bool hasIt = (spanIt != span.end() && *spanIt == e);

            if (hasIt) {
                if (_nodes[curIdx]._hasPt == INVALID_NODE) {
                    const PropertyTypeSet ptsCopy = _nodes[curIdx]._propertyTypeSet;
                    const uint16_t newIdx = allocNode();
                    _nodes[newIdx]._propertyTypeSet = ptsCopy;
                    _nodes[newIdx]._propertyTypeSet.add(ptId); // sorted: ptId > all previous
                    _nodes[curIdx]._hasPt = newIdx;
                }
                _coreEntityToNode[idx] = _nodes[curIdx]._hasPt;
                ++spanIt;
            } else {
                if (_nodes[curIdx]._lacksPt == INVALID_NODE) {
                    const PropertyTypeSet ptsCopy = _nodes[curIdx]._propertyTypeSet;
                    const uint16_t newIdx = allocNode();
                    _nodes[newIdx]._propertyTypeSet = ptsCopy;
                    _nodes[curIdx]._lacksPt = newIdx;
                }
                _coreEntityToNode[idx] = _nodes[curIdx]._lacksPt;
            }
        }
    }
}

const PropertyTypeSet& PropertyTypeTrie::getPropertyTypeSet(EntityID entityId) const {
    if (entityId < _firstCoreEntityID) {
        return _nodes[_patchEntityToNode.at(entityId)]._propertyTypeSet;
    }

    const size_t idx = (entityId - _firstCoreEntityID).getValue();
    if (idx >= _coreEntityCount || _coreEntityToNode[idx] == INVALID_NODE) {
        throw FatalException("Entity not found in PropertyTypeTrie");
    }

    return _nodes[_coreEntityToNode[idx]]._propertyTypeSet;
}

const PropertyTypeSet* PropertyTypeTrie::tryGetPropertyTypeSet(EntityID entityId) const {
    if (entityId < _firstCoreEntityID) {
        const auto it = _patchEntityToNode.find(entityId);
        if (it == _patchEntityToNode.end()) {
            return nullptr;
        }

        return &_nodes[it->second]._propertyTypeSet;
    }

    const size_t idx = (entityId - _firstCoreEntityID).getValue();
    if (idx >= _coreEntityCount || _coreEntityToNode[idx] == INVALID_NODE) {
        return nullptr;
    }

    return &_nodes[_coreEntityToNode[idx]]._propertyTypeSet;
}

bool PropertyTypeTrie::hasPropertyType(EntityID entityId, PropertyTypeID ptId) const {
    if (entityId < _firstCoreEntityID) {
        return _nodes[_patchEntityToNode.at(entityId)]._propertyTypeSet.contains(ptId);
    }

    const size_t idx = (entityId - _firstCoreEntityID).getValue();
    if (idx >= _coreEntityCount || _coreEntityToNode[idx] == INVALID_NODE) {
        throw FatalException("Entity not found in PropertyTypeTrie");
    }

    return _nodes[_coreEntityToNode[idx]]._propertyTypeSet.contains(ptId);
}

size_t PropertyTypeTrie::entityCount() const {
    size_t coreCount = 0;

    for (const uint16_t nodeIdx : _coreEntityToNode) {
        if (nodeIdx != INVALID_NODE) {
            ++coreCount;
        }
    }

    return _patchEntityToNode.size() + coreCount;
}

uint16_t PropertyTypeTrie::allocNode() {
    bioassert(_nodes.size() < INVALID_NODE, "PropertyTypeTrie node count exceeds uint16_t capacity");
    _nodes.push_back(TrieNode{});
    return static_cast<uint16_t>(_nodes.size() - 1);
}
