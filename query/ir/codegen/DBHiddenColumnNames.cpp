#include "DBHiddenColumnNames.h"

using namespace db;

namespace {

// The columns a CALL subquery carries past its body are bound under this prefix, so no
// clause of the body resolves or drops them - the columns are keyed by name.
constexpr std::string_view hiddenColumnPrefix {"`hidden_"};

}

std::string db::hiddenName(std::string_view name) {
    std::string hidden {hiddenColumnPrefix};
    hidden += name;
    return hidden;
}

bool db::isHiddenName(std::string_view name) {
    return name.starts_with(hiddenColumnPrefix);
}

std::string db::pendingMaskName(std::string_view entityName) {
    std::string name {pendingMaskPrefix};
    name += entityName;
    return name;
}
