#include "DebugDump.h"

#include <range/v3/view/drop.hpp>

#include "ListElementView.h"
#include "ListUtils.h"

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

void DebugDump::dump(std::ostream& out, ListElementView view, bool isInList) {
    const auto dumpTyped = [&out]<typename T>(const ListElementView ele) {
        const T typed = ele.getAs<T>();
        dump(out, typed);
    };

    const ListBufferTypeTag tag = view.getTag();
    ListTagDispatcher dumper {._tag = tag};
    dumper.execute(dumpTyped, view);

    if (isInList) {
        out << '\n';
    }
}

void DebugDump::dump(std::ostream& out, ListView list) {
    if (list.empty()) {
        out << "[]\n";
        return;
    }

    out << "[";

    constexpr bool isInList = true;

    const ListElementView fst = list.front();
    dump(out, fst, isInList);

    for (const ListElementView ele : list | rv::drop(1)) {
        dump(out, ele, isInList);
    }

    out << "]\n";
}

