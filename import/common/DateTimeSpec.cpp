#include "DateTimeSpec.h"

#include <string_view>

using namespace db;

bool DateTimeSpec::contains(std::string_view propName) const {
    return _names.contains(propName);
}
