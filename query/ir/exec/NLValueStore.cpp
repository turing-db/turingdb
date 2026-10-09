#include "NLValueStore.h"

#include <type_traits>
#include <variant>

#include "columns/ColumnConst.h"
#include "columns/ColumnVector.h"
#include "map/MapContainer.h"

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

ListElementView NLValueStore::own(ListElementView element) {
    _valueCount++;

    return getLists().copy(element);
}

MapEntryView NLValueStore::own(MapEntryView entry) {
    _valueCount++;

    return getLists().getMaps().copy(entry);
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

ListElementView NLValueStore::ownCell(const ListBuffer<>::ListItemVariant& item) {
    _valueCount++;

    const auto ownNested = [this](const auto& value) -> ListBuffer<>::ListItemVariant {
        using T = std::decay_t<decltype(value)>;

        if constexpr (std::same_as<T, ListView>) {
            return getLists().copy(value);
        } else if constexpr (std::same_as<T, MapView>) {
            return getLists().getMaps().copy(value);
        } else {
            return value;
        }
    };

    const ListBuffer<>::ListItemVariant nested = std::visit(ownNested, item);

    return getLists().insert(std::span<const ListBuffer<>::ListItemVariant> {&nested, 1}).front();
}

void NLValueStore::ownColumnRows(Column* column, size_t firstRow) {
    const auto ownTyped = [&]<typename ElementType>() {
        if (column->getKind() == ColumnVector<ElementType>::staticKind()) {
            std::vector<ElementType>& raw = static_cast<ColumnVector<ElementType>*>(column)->getRaw();

            for (size_t row = firstRow; row < raw.size(); row++) {
                raw[row] = ownElement(raw[row]);
            }
        } else if (column->getKind() == ColumnConst<ElementType>::staticKind()) {
            ElementType& constant = static_cast<ColumnConst<ElementType>*>(column)->getRaw();
            constant = ownElement(constant);
        }
    };

    for (const NLViewColumnKind kind : {NLViewColumnKind::String,
                                        NLViewColumnKind::OptString,
                                        NLViewColumnKind::OptEmbedding,
                                        NLViewColumnKind::List,
                                        NLViewColumnKind::OptList,
                                        NLViewColumnKind::Map,
                                        NLViewColumnKind::OptMap,
                                        NLViewColumnKind::ListElement,
                                        NLViewColumnKind::OptListElement,
                                        NLViewColumnKind::MapEntry}) {
        dispatchViewColumnKind(kind, ownTyped);
    }
}

void NLValueStore::clear() {
    _valueCount = 0;

    if (_strings) {
        _strings->clear();
    }

    if (_embeddings) {
        _embeddings->clear();
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
