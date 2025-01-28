#include "DebugDump.h"

#include <iostream>

using namespace db;

void DebugDump::dumpImpl(uint64_t data) {
    std::cout << data << '\n';
}

void DebugDump::dumpString(const std::string& str) {
    std::cout << str << '\n';
}

void DebugDump::dumpNull() {
    std::cout << "null\n";
}
