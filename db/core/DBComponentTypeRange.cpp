#include "DBComponentTypeRange.h"

using namespace db;

DBComponentTypeRange::DBComponentTypeRange(const DB* db)
    : _range(&db->_compTypes)
{
}
