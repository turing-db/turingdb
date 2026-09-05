#include "ListContainer.h"

#include <variant>

#include "ListElementView.h"
#include "ListUtils.h"

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
    _views(std::move(other._views))
{
}

ListContainer& ListContainer::operator=(ListContainer&& other) noexcept {
    _lists = std::move(other._lists);
    _strings = std::move(other._strings);
    _embeddings = std::move(other._embeddings);
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
    _views.clear();
}

ListView ListContainer::copy(ListView list) {
    std::vector<ListItemVariant> elements;
    elements.reserve(list.size());

    const auto asVariant = [this]<typename T>(const ListElementView view) -> ListItemVariant {
        if constexpr (std::same_as<T, ListView>) {
            return copy(view.getAs<ListView>());
        } else {
            return view.getAs<T>();
        }
    };

    for (const ListElementView element : list) {
        const ListTagDispatcher dispatcher {element.getTag()};
        elements.push_back(dispatcher.execute(asVariant, element));
    }

    return insert(elements);
}

ListContainer::ListItemVariant ListContainer::own(const ListItemVariant& element) {
    const auto copyPayload = [this](const auto& value) -> ListItemVariant {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::same_as<T, types::String::Primitive>) {
            return _strings->insert(value);
        } else if constexpr (std::same_as<T, types::Embedding::Primitive>) {
            return _embeddings->insert(value);
        } else {
            return value;
        }
    };

    return std::visit(copyPayload, element);
}
