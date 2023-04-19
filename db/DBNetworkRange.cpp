#include "DBNetworkRange.h"

using namespace db;

DBNetworkRange::DBNetworkRange(DB* db)
    : _range(&db->_networks)
{
}
