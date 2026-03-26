#include "PropertyManager.h"

#include <range/v3/view/enumerate.hpp>

#include "properties/PropertyTypeTrie.h"
#include "views/EntityPropertyView.h"

#include "BioAssert.h"

using namespace db;

PropertyManager::PropertyManager()
{
}

PropertyManager::~PropertyManager() {
}

void PropertyManager::buildTypeMapping(EntityID firstCoreEntityID,
                                       size_t coreEntityCount) {
    _typeMapping = std::make_unique<PropertyTypeTrie>();

    std::unordered_map<PropertyTypeID, std::span<const EntityID>> map;
    for (const auto& [ptID, container] : _map) {
        map.emplace(ptID, container->ids());
    }

    _typeMapping->build(map, firstCoreEntityID, coreEntityCount);
}

void PropertyManager::fillEntityPropertyView(EntityID entityID,
                                             const LabelSetHandle& labelset,
                                             EntityPropertyView& view) const {
    bioassert(labelset.isValid(), "Labelset must be valid");

    const auto fill = [&](const auto& container, PropertyTypeID ptID) {
        if (!_indexers.contains(ptID)) {
            return;
        }

        const auto* primitive = container.tryGet(entityID);
        if (primitive) {
            auto& prop = view._props.emplace_back();
            prop._id = ptID;
            prop._value = primitive;
        }
    };

    for (const auto& [ptID, rawContainer] : _uint64s) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::UInt64>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _int64s) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::Int64>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _doubles) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::Double>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _strings) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::String>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _bools) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::Bool>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _embeddings) {
        if (!has(ptID, entityID)) {
            continue;
        }

        const auto& container = rawContainer->cast<types::Embedding>();
        fill(container, ptID);
    }

}

const LabelSetPropertyIndexer* PropertyManager::tryGetIndexer(PropertyTypeID ptID) const {
    auto it = _indexers.find(ptID);
    if (it != _indexers.end()) {
        return &(it->second);
    }

    return nullptr;
}

