#include "DBEdgeTypeRange.h"

using namespace db;

DBEdgeTypeRange::DBEdgeTypeRange(const DB* db)
    : _range(&db->_edgeTypes)
{
}
