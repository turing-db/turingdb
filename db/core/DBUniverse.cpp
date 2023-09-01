#include "DBUniverse.h"

#include "BioLog.h"
using namespace Log;

using namespace db;

DBUniverse::DBUniverse()
{
}

DBUniverse::~DBUniverse() {
}

void DBUniverse::getDatabases(std::vector<std::string>& databases) const {
    BioLog::echo("LOCK BEGIN");
    std::shared_lock rd(_lock);
    BioLog::echo("LOCK ACQUIRED");
    for (const auto& entry : _databases) {
        databases.emplace_back(entry.first);
    }
}
