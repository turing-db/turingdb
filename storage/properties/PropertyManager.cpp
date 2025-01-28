#include "PropertyManager.h"

#include <range/v3/view/enumerate.hpp>

#include "views/EntityPropertyView.h"

using namespace db;

PropertyManager::PropertyManager(const GraphMetadata* graphMetadata)
    : _graphMetadata(graphMetadata)
{
}

PropertyManager::~PropertyManager() {
}

void PropertyManager::createEntity(LabelSetInfo& info,
                                   EntityID entityID,
                                   size_t offset) {
    if (!info._ranges.empty()) {
        auto& prevInfo = info._ranges.back();
        const EntityID nextID {prevInfo._firstID + prevInfo._count};
        if (nextID == entityID) {
            // Contiguous ids
            prevInfo._count++;
            return;
        }
    }

    if (info._ranges.size() > 3) {
        info._binarySearched = true;
    }

    // New range
    info._ranges.emplace_back(LabelSetRange {
        ._firstID = entityID,
        ._firstPos = offset,
        ._count = 1,
    });
}

void PropertyManager::fillEntityPropertyView(EntityID entityID,
                                             LabelSetID labelsetID,
                                             EntityPropertyView& view) const {
    msgbioassert(labelsetID.isValid(), "LabelsetID must be valid");

    const auto fill = [&](const auto& container, PropertyTypeID ptID) {
        const auto indexerIt = _indexers.find(ptID);
        if (indexerIt == _indexers.end()) {
            return;
        }

        const auto& indexer = indexerIt->second;

        // To speed up the search, we use the labelset
        const auto infoIt = indexer.find(labelsetID);
        if (infoIt == indexer.end()) {
            return;
        }

        const auto& info = infoIt->second;

        // If the container is fragmented, we perform a binary search
        if (info._binarySearched) {
            const auto* primitive = container.tryGet(entityID);
            if (primitive) {
                auto& prop = view._props.emplace_back();
                prop._id = ptID;
                prop._value = primitive;
            }
        } else {
            for (const auto& range : info._ranges) {
                if (range.entityInRange(entityID)) {
                    const size_t valueOffset = range.getOffset(entityID);
                    const auto& primitive = container.get(valueOffset);
                    auto& prop = view._props.emplace_back();
                    prop._id = ptID;
                    prop._value = &primitive;
                    break;
                }
            }
        }
    };

    for (const auto& [ptID, rawContainer] : _uint64s) {
        const auto& container = rawContainer->cast<types::UInt64>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _int64s) {
        const auto& container = rawContainer->cast<types::Int64>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _doubles) {
        const auto& container = rawContainer->cast<types::Double>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _strings) {
        const auto& container = rawContainer->cast<types::String>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _bools) {
        const auto& container = rawContainer->cast<types::Bool>();
        fill(container, ptID);
    }
}

const LabelSetIndexer<LabelSetInfo>* PropertyManager::tryGetIndexer(PropertyTypeID ptID) const {
    auto it = _indexers.find(ptID);
    if (it != _indexers.end()) {
        return &(it->second);
    }
    return nullptr;
}

