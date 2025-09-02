#pragma once

#include <vector>

namespace db {

class DataPart;

class DataPartComparator {
public:
    [[nodiscard]] static bool same(const DataPart& a, const DataPart& b);
    [[nodiscard]] bool correctlyMerged(const DataPart& merged,
                                       const std::vector<DataPart>& elements);
};

}
