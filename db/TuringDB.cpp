#include "TuringDB.h"

#include "QueryInterpreter.h"

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
    QueryInterpreter interp(&_systemManager);
    return interp.execute(query, graphName, mem, callback);
}

QueryStatus TuringDB::query(std::string_view query,
                             std::string_view graphName,
                             LocalMemory* mem) {
    QueryInterpreter interp(&_systemManager);
    return interp.execute(query, graphName, mem, [](const auto&){});
}
