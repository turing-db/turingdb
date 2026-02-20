#pragma once

#include <string>
#include <vector>

namespace db {

struct CSVFileInfo {
    std::vector<std::string> _headers;
    size_t _fieldCount {0};
};

}
