#include "MapContainer.h"

#include <algorithm>
#include <variant>

#include "MapEntryView.h"
#include "MapUtils.h"

using namespace db;

MapContainer::MapContainer()
    : _maps(std::make_unique<MapBuffer<>>()),
    _strings(std::make_unique<StringBuffer>()),
    _embeddings(std::make_unique<EmbeddingBuffer>())
{
}

MapContainer::~MapContainer() {
}

MapContainer::MapContainer(MapContainer&& other) noexcept
    : _maps(std::move(other._maps)),
    _strings(std::move(other._strings)),
    _embeddings(std::move(other._embeddings)),
    _lists(std::move(other._lists)),
    _views(std::move(other._views))
{
}

MapContainer& MapContainer::operator=(MapContainer&& other) noexcept {
    _maps = std::move(other._maps);
    _strings = std::move(other._strings);
    _embeddings = std::move(other._embeddings);
    _lists = std::move(other._lists);
    _views = std::move(other._views);
    return *this;
}

void MapContainer::alloc(MapView map) {
    append(copy(map));
}

MapView MapContainer::insert(std::span<const MapKeyValuePair> entries) {
    std::vector<MapKeyValuePair> owned;
    owned.reserve(entries.size());

    for (const MapKeyValuePair& entry : entries) {
        owned.push_back(own(entry));
    }

    std::stable_sort(owned.begin(), owned.end(), [](const MapKeyValuePair& lhs, const MapKeyValuePair& rhs) {
        return lhs.key < rhs.key;
    });

    return _maps->insert(owned);
}

void MapContainer::append(MapView map) {
    _views.push_back(map);
}

void MapContainer::clear() {
    _maps->clear();
    _strings->clear();
    _embeddings->clear();
    _lists.clear();
    _views.clear();
}

MapView MapContainer::copy(MapView map) {
    std::vector<MapKeyValuePair> entries;
    entries.reserve(map.size());

    const auto asVariant = [this]<typename T>(const MapEntryView view) -> MapBuffer<>::MapItemVariant {
        if constexpr (std::same_as<T, MapView>) {
            return copy(view.getValueAs<MapView>());
        } else if constexpr (std::same_as<T, ListView>) {
            return _lists.copy(view.getValueAs<ListView>());
        } else {
            return view.getValueAs<T>();
        }
    };

    for (const MapEntryView entry : map) {
        const MapTagDispatcher dispatcher {entry.getValueTag()};
        entries.push_back({entry.getKey(), dispatcher.execute(asVariant, entry)});
    }

    return insert(entries);
}

MapContainer::MapKeyValuePair MapContainer::own(const MapKeyValuePair& entry) {
    const auto copyPayload = [this](const auto& value) -> MapBuffer<>::MapItemVariant {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::same_as<T, types::String::Primitive>) {
            return _strings->insert(value);
        } else if constexpr (std::same_as<T, types::Embedding::Primitive>) {
            return _embeddings->insert(value);
        } else {
            return value;
        }
    };

    return {_strings->insert(entry.key), std::visit(copyPayload, entry.value)};
}
