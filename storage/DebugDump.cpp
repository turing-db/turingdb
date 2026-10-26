#include "DebugDump.h"

#include <sstream>

#include <range/v3/view/drop.hpp>

#include "CellView.h"
#include "CellUtils.h"

#include "map/MapEntryView.h"
#include "map/MapView.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

void DebugDump::dump(std::ostream& out, std::string_view str) {
    out << str << '\n';
}

void DebugDump::dump(std::ostream& out, const std::string& str) {
    out << str << '\n';
}

void DebugDump::dumpImpl(std::ostream& out, uint64_t data) {
    out << data << '\n';
}

void DebugDump::dumpString(std::ostream& out, const std::string& str) {
    out << str << '\n';
}

void DebugDump::dumpNull(std::ostream& out) {
    out << "null\n";
}

void DebugDump::dump(std::ostream& out, CellView view, bool isInList) {
    const auto dumpTyped = [&out, isInList]<typename T>(const CellView ele) {
        const T typed = ele.getAs<T>();

        // Dumping a column of CellViews:
        if (!isInList) {
            dump(out, typed);
            return;
        }

        // Dumping a column of ListViews: remove the \n after each element
        std::ostringstream temp;
        dump(temp, typed);
        std::string dumped = temp.str();
        if (!dumped.empty() && dumped.back() == '\n') {
            dumped.pop_back();
        }

        out << dumped;
    };

    const CellTag tag = view.getTag();
    CellTagDispatcher dumper {._tag = tag};
    dumper.execute(dumpTyped, view);
}

void DebugDump::dump(std::ostream& out, ListView list) {
    if (list.empty()) {
        out << "[]\n";
        return;
    }

    out << "[";

    constexpr bool isInList = true;

    const CellView fst = list.front();
    dump(out, fst, isInList);

    for (const CellView ele : list | rv::drop(1)) {
        out << ", ";
        dump(out, ele, isInList);
    }

    out << "]\n";
}

void DebugDump::dump(std::ostream& out, MapEntryView view, bool isInMap) {
    out << view.getKey() << ": ";

    dump(out, view.getValue(), isInMap);
}

void DebugDump::dump(std::ostream& out, MapView map) {
    if (map.empty()) {
        out << "{}\n";
        return;
    }

    out << "{";

    constexpr bool isInMap = true;

    const MapEntryView fst = map.front();
    dump(out, fst, isInMap);

    for (const MapEntryView entry : map | rv::drop(1)) {
        out << ", ";
        dump(out, entry, isInMap);
    }

    out << "}\n";
}

