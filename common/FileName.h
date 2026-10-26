#pragma once

#include <stddef.h>

#include <string_view>

// A graph or an extension is stored under its own name in a directory the engine owns. That
// name holds letters, digits and '_' only, so no separator or dot can reach outside it
class FileName {
public:
    FileName() = delete;

    // The position of the first character a stored name cannot hold, npos when there is none
    static size_t findInvalidCharacter(std::string_view name);
};
