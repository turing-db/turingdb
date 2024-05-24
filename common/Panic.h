#pragma once


[[noreturn]] inline void panic() {
    throw "panic";
}
