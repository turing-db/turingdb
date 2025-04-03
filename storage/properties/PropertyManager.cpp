#include "PropertyManager.h"

#include <range/v3/view/enumerate.hpp>

#include "views/EntityPropertyView.h"

using namespace db;

PropertyManager::PropertyManager()
{
}

PropertyManager::~PropertyManager() {
}

void PropertyManager::fillEntityPropertyView(EntityID entityID,
                                             const LabelSetHandle& labelset,
                                             EntityPropertyView& view) const {
    msgbioassert(labelset.isValid(), "Labelset must be valid");

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

const LabelSetPropertyIndexer* PropertyManager::tryGetIndexer(PropertyTypeID ptID) const {
    auto it = _indexers.find(ptID);
    if (it != _indexers.end()) {
        return &(it->second);
    }
    return nullptr;
}

