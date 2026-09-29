#include "DurationSpec.h"

#include <string_view>

using namespace db;

bool DurationSpec::contains(std::string_view propertyName) const {
    return _names.contains(propertyName);
}
