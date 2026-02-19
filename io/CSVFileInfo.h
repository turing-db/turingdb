#pragma once

#include <string>
#include <vector>

namespace db {

struct CSVFileInfo {
    std::vector<std::string> headers;
    size_t fieldCount {0};
};

}
