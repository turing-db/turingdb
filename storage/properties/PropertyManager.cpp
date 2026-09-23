#include "PropertyManager.h"

#include <range/v3/view/enumerate.hpp>

#include "views/EntityPropertyView.h"

#include "BioAssert.h"

using namespace db;

PropertyManager::PropertyManager()
{
}

PropertyManager::~PropertyManager() {
}

void PropertyManager::fillEntityPropertyView(EntityID entityID,
                                             const LabelSetHandle& labelset,
                                             EntityPropertyView& view) const {
    bioassert(labelset.isValid(), "Labelset must be valid");

    const auto fill = [&](const auto& container, PropertyTypeID ptID) {
        if (!_indexers.contains(ptID)) {
            return;
        }

        const auto value = container.tryGetWithNull(entityID);

        const bool explicitNull = !value.has_value();
        if (explicitNull) {
            view.removeProperty(ptID);
            return;
        }

        const auto* primitive = value.value();
        if (primitive) {
            view.setProperty(ptID, primitive);
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

    for (const auto& [ptID, rawContainer] : _embeddings) {
        const auto& container = rawContainer->cast<types::Embedding>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _lists) {
        const auto& container = rawContainer->cast<types::List>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _dateTimes) {
        const auto& container = rawContainer->cast<types::DateTime>();
        fill(container, ptID);
    }

    for (const auto& [ptID, rawContainer] : _maps) {
        const auto& container = rawContainer->cast<types::Map>();
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

