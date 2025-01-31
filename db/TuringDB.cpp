#include "TuringDB.h"

using namespace db;

TuringDB::TuringDB()
{
}

TuringDB::~TuringDB()
{
}

QueryStatus TuringDB::query(std::string_view query,
                             std::string_view graphName,
                             LocalMemory* mem,
                             QueryCallback callback) {
    return QueryStatus(QueryStatus::Status::OK);
}

QueryStatus TuringDB::query(std::string_view query,
                             std::string_view graphName,
                             LocalMemory* mem) {
    return QueryStatus(QueryStatus::Status::OK);
}
