#include "NLValueStore.h"

#include <type_traits>
#include <variant>

#include "columns/ColumnConst.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "map/MapContainer.h"

#include "Overloaded.h"

using namespace db;

namespace {

// The copies a store may hold beyond twice the live values before it is compacted, so a
// handful of live values is not compacted on every step
constexpr size_t COMPACTION_SLACK = 4096;

}

NLValueStore::NLValueStore()
{
}

NLValueStore::~NLValueStore() {
}

types::String::Primitive NLValueStore::own(types::String::Primitive text) {
    _valueCount++;

    return getStrings().insert(text);
}

types::Embedding::Primitive NLValueStore::own(types::Embedding::Primitive embedding) {
    _valueCount++;

    return getEmbeddings().insert(embedding);
}

ListView NLValueStore::own(ListView list) {
    _valueCount++;

    return getLists().copy(list);
}

MapView NLValueStore::own(MapView map) {
    _valueCount++;

    return getLists().getMaps().copy(map);
}

PathView NLValueStore::own(PathView path) {
    _valueCount++;

    return getPaths().insert(path.getIDs());
}

CellView NLValueStore::own(CellView element) {
    _valueCount++;

    return getLists().copy(element);
}

ListView NLValueStore::decode(const EncodedList& list) {
    _valueCount++;

    return list.decodeInto(getLists());
}

MapView NLValueStore::decode(const EncodedMap& map) {
    _valueCount++;

    return map.decodeInto(getLists().getMaps());
}

ListBuffer<>::ListItemVariant NLValueStore::ownItem(const ListBuffer<>::ListItemVariant& item) {
    return std::visit([this](const auto& value) -> ListBuffer<>::ListItemVariant { return ownElement(value); }, item);
}

CellView NLValueStore::ownCell(ListBuffer<>::ListItemVariant& item) {
    _valueCount++;

    const auto ownNested = [this](auto& value) {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::same_as<T, ListView>) {
            value = getLists().copy(value);
        } else if constexpr (std::same_as<T, MapView>) {
            value = getLists().getMaps().copy(value);
        }
    };

    std::visit(ownNested, item);

    return getLists().insert(std::span<const ListBuffer<>::ListItemVariant> {&item, 1}).front();
}

void NLValueStore::ownColumnRows(Column* column, size_t firstRow) {
    dispatchViewColumn(column, Overloaded {
        [&]<typename ElementType>(const ColumnConst<ElementType>*) {
            ColumnConst<ElementType>* constant = static_cast<ColumnConst<ElementType>*>(column);

            if constexpr (ColumnConst<ElementType>::IS_NULLABLE) {
                if (constant->isValid(0)) {
                    constant->set(own(constant->getValue(0)));
                }
            } else {
                ElementType& value = constant->getRaw();
                value = own(value);
            }
        },
        [&]<typename ElementType>(const ColumnVector<ElementType>*) {
            ownRows(static_cast<ColumnVector<ElementType>*>(column), firstRow);
        },
        [&]<typename Element>(const ColumnOptVector<Element>*) {
            ownRows(static_cast<ColumnOptVector<Element>*>(column), firstRow);
        },
    });
}

void NLValueStore::clear() {
    _valueCount = 0;

    if (_strings) {
        _strings->clear();
    }

    if (_embeddings) {
        _embeddings->clear();
    }

    if (_paths) {
        _paths->clear();
    }

    if (_lists) {
        _lists->clear();
    }
}

StringBuffer& NLValueStore::getStrings() {
    if (!_strings) {
        _strings = std::make_unique<StringBuffer>();
    }

    return *_strings;
}

ListContainer::EmbeddingBuffer& NLValueStore::getEmbeddings() {
    if (!_embeddings) {
        _embeddings = std::make_unique<ListContainer::EmbeddingBuffer>();
    }

    return *_embeddings;
}

ListContainer::PathBuffer& NLValueStore::getPaths() {
    if (!_paths) {
        _paths = std::make_unique<ListContainer::PathBuffer>();
    }

    return *_paths;
}

ListContainer& NLValueStore::getLists() {
    if (!_lists) {
        _lists = std::make_unique<ListContainer>();
    }

    return *_lists;
}

bool NLCompactingValueStore::needsCompaction(size_t liveCount) const {
    return _stores[_active].getValueCount() > 2 * liveCount + COMPACTION_SLACK;
}

NLValueStore& NLCompactingValueStore::getSpare() {
    NLValueStore& spare = _stores[1 - _active];
    spare.clear();

    return spare;
}

void NLCompactingValueStore::flip() {
    _stores[_active].clear();
    _active = 1 - _active;
}

void NLCompactingValueStore::clear() {
    _stores[0].clear();
    _stores[1].clear();
}
