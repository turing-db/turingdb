#include "DBNodeTypeRange.h"

using namespace db;

DBNodeTypeRange::DBNodeTypeRange(const DB* db)
    : _range(&db->_nodeTypes)
{
}
