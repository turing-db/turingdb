#pragma once

#include "Panic.h"

template<typename T, typename U>
T RequireCast(U* value) {
    if (auto casted = dynamic_cast<T>(value)) {
        return casted;
    } else {
        panic("Cast failed");
    }
}
