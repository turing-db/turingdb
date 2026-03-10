#include "PropertyContainer.h"

using namespace db;

void TypedPropertyContainer<types::Embedding>::add(EntityID entityID, std::span<const float> v) {
    _values.alloc(v);
    _ids.emplace_back(entityID);
}


void TypedPropertyContainer<types::String>::sort() {
    StringContainer newValues;
    if (_ids.empty()) {
        return;
    }

    std::vector<size_t> offsets(_ids.size());
    std::iota(offsets.begin(), offsets.end(), 0);

    ranges::sort(
        ranges::views::zip(_ids, offsets),
        [&](const auto& pair1, const auto& pair2) {
            const EntityID id1 = std::get<0>(pair1);
            const EntityID id2 = std::get<0>(pair2);
            return id1 < id2;
        });

    for (size_t i : offsets) {
        newValues.alloc(_values.getView(i));
    }

    _values = std::move(newValues);

    _entityIndexMap.clear();
    _entityIndexMap.reserve(_ids.size());
    for (size_t i = 0; i < _ids.size(); i++) {
        _entityIndexMap[_ids[i]] = i;
    }
}

void TypedPropertyContainer<types::Embedding>::sort() {
    EmbeddingContainer newValues(_values.dimension());
    if (_ids.empty()) {
        return;
    }

    std::vector<size_t> offsets(_ids.size());
    std::iota(offsets.begin(), offsets.end(), 0);

    ranges::sort(
        ranges::views::zip(_ids, offsets),
        [&](const auto& pair1, const auto& pair2) {
            const EntityID id1 = std::get<0>(pair1);
            const EntityID id2 = std::get<0>(pair2);
            return id1 < id2;
        });

    for (size_t i : offsets) {
        newValues.alloc(_values.getView(i));
    }

    _values = std::move(newValues);

    // Detect dense IDs
    _firstID = _ids.front();
    _isDense = (_ids.back().getValue() - _ids.front().getValue() + 1) == _ids.size();

    if (!_isDense) {
        _entityIndexMap.clear();
        _entityIndexMap.reserve(_ids.size());
        for (size_t i = 0; i < _ids.size(); i++) {
            _entityIndexMap[_ids[i]] = i;
        }
    }
}
