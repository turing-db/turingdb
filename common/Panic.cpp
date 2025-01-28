#include "Panic.h"

void panic() {
    throw TuringDBException();
}

void panic(std::string&& msg) {
    throw TuringDBException(std::move(msg));
}

