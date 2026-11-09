#include "ParameterValueReader.h"

#include <stdint.h>
#include <concepts>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParameterValue.h"

#include "CellUtils.h"
#include "CellView.h"
#include "columns/AllowedKinds.h"
#include "columns/ColumnConst.h"
#include "columns/ColumnMask.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnVector.h"
#include "ID.h"
#include "PathView.h"
#include "list/ListView.h"
#include "map/MapView.h"
#include "metadata/PropertyNull.h"

#include "NameOf.h"
#include "TuringException.h"

using namespace db;

namespace {

using Item = std::pair<CellView, ParameterValue*>;

[[noreturn]] void throwNotAValue(std::string_view what) {
    throw TuringException(fmt::format("A parameter cannot hold {}", what));
}

// Reads one cell into a value. A list or a map is sized here and its elements are queued,
// so a value nests as deep as the result does without recursion.
class CellReader {
public:
    CellReader(ParameterValue& value, std::vector<Item>& worklist)
        : _value(value),
        _worklist(worklist)
    {
    }

    void read(int64_t integer) { _value.setInt64(integer); }
    void read(double real) { _value.setDouble(real); }
    void read(bool boolean) { _value.setBool(boolean); }
    void read(CustomBool boolean) { _value.setBool(boolean._boolean); }
    void read(std::string_view text) { _value.setString(text); }
    void read(const std::string& text) { _value.setString(text); }
    void read(DateTime instant) { _value.setDateTime(instant); }
    void read(Duration duration) { _value.setDuration(duration); }
    void read(PropertyNull) { _value.setNull(); }
    void read(NodeID) { throwNotAValue("a node"); }
    void read(EdgeID) { throwNotAValue("an edge"); }
    void read(PathView) { throwNotAValue("a path"); }
    void read(std::span<const float> embedding) { _value.setEmbedding(embedding); }

    void read(ListView list) {
        ParameterValue::List& elements = _value.setList();
        elements.resize(list.size());

        size_t index = 0;
        for (const CellView cell : list.elements()) {
            _worklist.emplace_back(cell, &elements[index]);
            index++;
        }
    }

    void read(MapView map) {
        ParameterValue::Map& entries = _value.setMap();

        for (const MapEntryView entry : map.entries()) {
            _worklist.emplace_back(entry.getValue(), &entries[std::string(entry.getKey())]);
        }
    }

    void read(CellView cell) {
        const auto readTyped = [this]<typename T>(const CellView view) {
            read(view.getAs<T>());
        };

        const CellTagDispatcher dispatcher {._tag = cell.getTag()};
        dispatcher.execute(readTyped, cell);
    }

    template <typename T>
    void read(const std::optional<T>& maybe) {
        if (maybe) {
            read(*maybe);
        } else {
            _value.setNull();
        }
    }

    template <typename T>
    void read(const T& other) {
        if constexpr (std::unsigned_integral<T>) {
            if (other > static_cast<T>(std::numeric_limits<int64_t>::max())) {
                throw TuringException(fmt::format("Integer {} exceeds the Int64 range", other));
            }

            _value.setInt64(static_cast<int64_t>(other));
        } else {
            throwNotAValue(fmt::format("a value of type {}", NameOf<T>::get()));
        }
    }

private:
    ParameterValue& _value;
    std::vector<Item>& _worklist;
};

class ColumnValueReader {
public:
    ColumnValueReader(size_t row, ParameterValue& value, std::vector<Item>& worklist)
        : _row(row),
        _value(value),
        _worklist(worklist)
    {
    }

    template <typename T>
    void operator()(const ColumnVector<T>* column) {
        CellReader reader(_value, _worklist);
        reader.read(column->at(_row));
    }

    template <typename T>
    void operator()(const ColumnConst<T>* column) {
        CellReader reader(_value, _worklist);
        reader.read(column->at(_row));
    }

    void operator()(const ColumnMask* column) {
        CellReader reader(_value, _worklist);
        reader.read(column->at(_row));
    }

private:
    size_t _row {0};
    ParameterValue& _value;
    std::vector<Item>& _worklist;
};

}

void db::readParameterValue(const Column* column, size_t row, ParameterValue& value) {
    std::vector<Item> worklist;

    ColumnValueReader reader(row, value, worklist);
    using Dispatcher = ColumnSingleDispatcher<OutputtedTypes::Allowed, ColumnValueReader, OutputtedTypes::Excluded>;
    Dispatcher::dispatch(column, reader);

    while (!worklist.empty()) {
        const auto [cell, target] = worklist.back();
        worklist.pop_back();

        CellReader cellReader(*target, worklist);
        cellReader.read(cell);
    }
}
