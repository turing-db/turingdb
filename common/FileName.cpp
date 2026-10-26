#include "FileName.h"

#include <ctype.h>

size_t FileName::findInvalidCharacter(std::string_view name) {
    for (size_t position = 0; position < name.size(); ++position) {
        const unsigned char c = static_cast<unsigned char>(name[position]);
        if (!isalnum(c) && c != '_') {
            return position;
        }
    }

    return std::string_view::npos;
}
