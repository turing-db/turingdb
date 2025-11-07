#pragma once

#include <vector>
#include <string_view>

#include "ColumnTagManager.h"
#include "ColumnTag.h"

namespace db {

class NamedColumn;

class DataframeManager {
public:
    friend NamedColumn;

    DataframeManager();
    ~DataframeManager();

    ColumnTagManager* getTagManager() { return &_tagManager; }

    ColumnTag allocTag() { return _tagManager.allocTag(); }

    std::string_view getTagName(ColumnTag tag) const {
        const size_t tagValue = tag.getValue();
        if (tagValue >= _tagNames.size()) {
            return {};
        }
        
        return _tagNames[tagValue];
    }

    void setTagName(ColumnTag tag, std::string_view name);

private:
    ColumnTagManager _tagManager;
    std::vector<NamedColumn*> _columns;
    std::vector<std::string_view> _tagNames;

    void addColumn(NamedColumn* column);
};

}
