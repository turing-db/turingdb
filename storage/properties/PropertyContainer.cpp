#include "PropertyContainer.h"

#include <numeric>

#include <range/v3/algorithm/sort.hpp>
#include <range/v3/view/zip.hpp>

using namespace db;

void TypedPropertyContainer<types::String>::sort() {
    if (_ids.empty()) {
        _sorted = true;
        return;
    }

    StringContainer newValues;

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

    _sorted = true;
}

void TypedPropertyContainer<types::Embedding>::sort() {
    if (_ids.empty()) {
        _sorted = true;
        return;
    }

    const size_t dimension = _values.getDimension();
    EmbeddingContainer newValues(dimension);

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

    _sorted = true;
}
