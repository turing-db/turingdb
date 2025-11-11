#pragma once

#include <vector>

#include "ColumnTagManager.h"

namespace db {

class NamedColumn;

class DataframeManager {
public:
    friend NamedColumn;

    DataframeManager();
    ~DataframeManager();

    ColumnTagManager& getTagManager() { return _tagManager; }

    ColumnTag allocTag() { return _tagManager.allocTag(); }

private:
    ColumnTagManager _tagManager;
    std::vector<NamedColumn*> _columns;

    void addColumn(NamedColumn* column);
};

}
