#include "ListContainer.h"

#include <variant>

#include "CellView.h"
#include "CellUtils.h"

#include "map/MapContainer.h"

using namespace db;

ListContainer::ListContainer()
    : _lists(std::make_unique<ListBuffer<>>()),
    _strings(std::make_unique<StringBuffer>()),
    _embeddings(std::make_unique<EmbeddingBuffer>())
{
}

ListContainer::~ListContainer() {
}

ListContainer::ListContainer(ListContainer&& other) noexcept
    : _lists(std::move(other._lists)),
    _strings(std::move(other._strings)),
    _embeddings(std::move(other._embeddings)),
    _paths(std::move(other._paths)),
    _maps(std::move(other._maps)),
    _views(std::move(other._views))
{
}

ListContainer& ListContainer::operator=(ListContainer&& other) noexcept {
    _lists = std::move(other._lists);
    _strings = std::move(other._strings);
    _embeddings = std::move(other._embeddings);
    _paths = std::move(other._paths);
    _maps = std::move(other._maps);
    _views = std::move(other._views);
    return *this;
}

void ListContainer::alloc(ListView list) {
    append(copy(list));
}

ListView ListContainer::insert(std::span<const ListItemVariant> elements) {
    std::vector<ListItemVariant> owned;
    owned.reserve(elements.size());

    for (const ListItemVariant& element : elements) {
        owned.push_back(own(element));
    }

    return _lists->insert(owned);
}

void ListContainer::append(ListView list) {
    _views.push_back(list);
}

void ListContainer::clear() {
    _lists->clear();
    _strings->clear();
    _embeddings->clear();
    if (_paths) {
        _paths->clear();
    }
    if (_maps) {
        _maps->clear();
    }
    _views.clear();
}

MapContainer& ListContainer::getMaps() {
    if (!_maps) {
        _maps = std::make_unique<MapContainer>();
    }

    return *_maps;
}

ListView ListContainer::copy(ListView list) {
    std::vector<ListItemVariant> elements;
    elements.reserve(list.size());

    for (const CellView element : list) {
        elements.push_back(ownedItem(element));
    }

    return insert(elements);
}

CellView ListContainer::copy(CellView element) {
    const ListItemVariant item = ownedItem(element);

    return insert(std::span<const ListItemVariant> {&item, 1}).front();
}

ListContainer::ListItemVariant ListContainer::ownedItem(CellView element) {
    const auto asVariant = [this]<typename T>(const CellView view) -> ListItemVariant {
        if constexpr (std::same_as<T, ListView>) {
            return copy(view.getAs<ListView>());
        } else if constexpr (std::same_as<T, MapView>) {
            return getMaps().copy(view.getAs<MapView>());
        } else {
            return view.getAs<T>();
        }
    };

    const CellTagDispatcher dispatcher {element.getTag()};

    return dispatcher.execute(asVariant, element);
}

ListContainer::PathBuffer& ListContainer::getPaths() {
    if (!_paths) {
        _paths = std::make_unique<PathBuffer>();
    }

    return *_paths;
}

ListContainer::ListItemVariant ListContainer::own(const ListItemVariant& element) {
    const auto copyPayload = [this](const auto& value) -> ListItemVariant {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::same_as<T, types::String::Primitive>) {
            return _strings->insert(value);
        } else if constexpr (std::same_as<T, types::Embedding::Primitive>) {
            return _embeddings->insert(value);
        } else if constexpr (std::same_as<T, PathView>) {
            return getPaths().insert(value.getIDs());
        } else {
            return value;
        }
    };

    return std::visit(copyPayload, element);
}
