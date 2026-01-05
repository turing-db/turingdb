#pragma once

#include <string_view>

#include "dataframe/ColumnTag.h"

namespace db::v2 {

struct ProjectionItem {
    ColumnTag _tag;
    std::string_view _name;
};

}
