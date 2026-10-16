#include "TuringProtoParameterWriter.h"

#include <algorithm>
#include <variant>
#include <vector>

#include "ParameterMap.h"
#include "ParameterValue.h"

using namespace net::proto;

namespace {

using ParameterEntry = db::ParameterMap::Values::value_type;
using BuiltValue = std::variant<const db::ParameterValue*, db::ListView, db::MapView>;

struct BuildFrame {
    const db::ParameterValue* _value {nullptr};
    bool _childrenBuilt {false};
};

bool holdsContainer(const db::ParameterValue& value) {
    if (value.isNull()) {
        return false;
    }

    const db::ValueType type = value.getType();
    return type == db::ValueType::List || type == db::ValueType::Map;
}

template <typename Item>
void toStoredScalar(const db::ParameterValue& value, Item& item) {
    if (value.isNull()) {
        item = db::PropertyNull {};
        return;
    }

    switch (value.getType()) {
        case db::ValueType::Int64:
            item = db::types::Int64::Primitive {value.getInt64()};
        break;
        case db::ValueType::Double:
            item = db::types::Double::Primitive {value.getDouble()};
        break;
        case db::ValueType::Bool:
            item = db::types::Bool::Primitive {value.getBool()};
        break;
        case db::ValueType::String:
            item = db::types::String::Primitive {value.getString()};
        break;
        default:
            bioassert(false, "Parameter of type {} is not a scalar", value.getTypeName());
        break;
    }
}

template <typename Item>
void toStoredItem(const BuiltValue& built, Item& item) {
    if (const db::ListView* list = std::get_if<db::ListView>(&built)) {
        item = *list;
    } else if (const db::MapView* map = std::get_if<db::MapView>(&built)) {
        item = *map;
    } else {
        toStoredScalar(*std::get<const db::ParameterValue*>(built), item);
    }
}

// Builds every container under the root bottom-up, children before their parent, and leaves
// the root's built value on top of the built stack.
void buildValue(const db::ParameterValue& root,
                db::QueryListBuffer& listBuffer,
                db::MapBuffer<>& mapBuffer,
                std::vector<BuiltValue>& built) {
    std::vector<BuildFrame> worklist;
    worklist.push_back({&root, false});

    std::vector<db::QueryListBuffer::ListItemVariant> elements;
    std::vector<db::MapBuffer<>::MapKeyValuePair> entries;

    while (!worklist.empty()) {
        const BuildFrame frame = worklist.back();
        worklist.pop_back();

        const db::ParameterValue& value = *frame._value;

        if (!holdsContainer(value)) {
            built.emplace_back(&value);
        } else if (!frame._childrenBuilt) {
            worklist.push_back({&value, true});

            if (value.getType() == db::ValueType::List) {
                const db::ParameterValue::List& list = value.getList();
                for (auto element = list.rbegin(); element != list.rend(); element++) {
                    worklist.push_back({&*element, false});
                }
            } else {
                const db::ParameterValue::Map& map = value.getMap();
                for (auto entry = map.rbegin(); entry != map.rend(); entry++) {
                    worklist.push_back({&entry->second, false});
                }
            }
        } else if (value.getType() == db::ValueType::List) {
            const size_t count = value.getList().size();
            const size_t first = built.size() - count;

            elements.resize(count);
            for (size_t index = 0; index < count; index++) {
                toStoredItem(built[first + index], elements[index]);
            }

            built.resize(first);
            built.emplace_back(listBuffer.insert(elements));
        } else {
            const db::ParameterValue::Map& map = value.getMap();
            const size_t first = built.size() - map.size();

            entries.resize(map.size());
            size_t index = 0;
            for (const auto& [key, entry] : map) {
                entries[index].key = key;
                toStoredItem(built[first + index], entries[index].value);
                index++;
            }

            built.resize(first);
            built.emplace_back(mapBuffer.insert(entries));
        }
    }
}

}

TuringProtoParameterWriter::TuringProtoParameterWriter(TuringProtoOutBuf* outBuf)
    : _writer(outBuf)
{
}

TuringProtoParameterWriter::~TuringProtoParameterWriter() {
}

void TuringProtoParameterWriter::writeParameters(const db::ParameterMap& parameters) {
    _listBuffer.clear();
    _mapBuffer.clear();

    const db::ParameterMap::Values& values = parameters.getValues();

    std::vector<const ParameterEntry*> sortedEntries;
    sortedEntries.reserve(values.size());
    for (const ParameterEntry& entry : values) {
        sortedEntries.push_back(&entry);
    }

    std::sort(sortedEntries.begin(), sortedEntries.end(), [](const ParameterEntry* lhs, const ParameterEntry* rhs) {
        return lhs->first < rhs->first;
    });

    std::vector<BuiltValue> built;
    std::vector<db::MapBuffer<>::MapKeyValuePair> entries(sortedEntries.size());

    for (size_t index = 0; index < sortedEntries.size(); index++) {
        const ParameterEntry& entry = *sortedEntries[index];

        buildValue(entry.second, _listBuffer, _mapBuffer, built);

        entries[index].key = entry.first;
        toStoredItem(built.back(), entries[index].value);
        built.pop_back();
    }

    _writer.writeMapView(_mapBuffer.insert(entries));
}
