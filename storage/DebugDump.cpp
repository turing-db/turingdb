#include "DebugDump.h"

using namespace db;

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
