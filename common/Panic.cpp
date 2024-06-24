#include "Panic.h"

void panic() {
    throw TuringException();
}

void panic(std::string&& msg) {
    throw TuringException(std::move(msg));
}

