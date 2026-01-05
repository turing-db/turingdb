#pragma once

#include <string_view>

#include "dataframe/ColumnTag.h"

namespace db {

struct ProjectionItem {
    ColumnTag _tag;
    std::string_view _name;
};

}
