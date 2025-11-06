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
