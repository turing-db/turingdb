#pragma once

#include <vector>

namespace db {

class NamedColumn;

class DataframeManager {
public:
    friend NamedColumn;

    DataframeManager();
    ~DataframeManager();

private:
    std::vector<NamedColumn*> _columns;

    void addColumn(NamedColumn* column);
};

}
