#include "DataframeManager.h"

#include "NamedColumn.h"

using namespace db;

DataframeManager::DataframeManager()
{
}

DataframeManager::~DataframeManager() {
    for (NamedColumn* col : _columns) {
        delete col;
    }
}

void DataframeManager::addColumn(NamedColumn* column) {
    _columns.push_back(column);
}

void DataframeManager::setTagName(ColumnTag tag, std::string_view name) {
    const size_t tagValue = tag.getValue();
    if (tagValue >= _tagNames.size()) {
        _tagNames.resize(tagValue+1);
    }

    _tagNames[tagValue] = name;
}
