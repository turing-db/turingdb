#pragma once

#include <string_view>
#include <vector>

namespace db::v2 {

class Expr;

struct EntityProperty {
    std::string_view propTypeName;
    Expr* expr {nullptr};
};

struct EntityProperties {
    std::vector<EntityProperty> properties;
};

}
