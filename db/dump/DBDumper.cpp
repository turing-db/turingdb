#include "DBDumper.h"

#include <spdlog/spdlog.h>

#include "DB.h"
#include "FileUtils.h"
#include "StringIndexDumper.h"
#include "EntityDumper.h"
#include "TimerStat.h"
#include "TypeDumper.h"

#include "LogUtils.h"

using namespace db;

DBDumper::DBDumper(const DB* db, const Path& dbPath)
    : _dbPath(dbPath),
      _db(db)
{
}

DBDumper::~DBDumper() = default;

bool DBDumper::dump() {
    TimerStat timer {"Dumping Turing db: " + _dbPath.string() };

    Path stringIndexPath = _dbPath / "smap";
    Path typeIndexPath = _dbPath / "types";
    Path entityIndexPath = _dbPath / "data";

    spdlog::info("Dumping database in {}", _dbPath.string());

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(_dbPath)) {
        if (!FileUtils::removeDirectory(_dbPath)) {
            logt::CanNotRemove(_dbPath.string());
            return false;
        }
    }

    if (!FileUtils::createDirectory(_dbPath)) {
        logt::CanNotCreateDir(_dbPath.string());
        return false;
    }

    StringIndexDumper strDumper{stringIndexPath};
    if (!strDumper.dump(_db->_strIndex)) {
        spdlog::error("Error while dumping the database string index in {}",
                      _dbPath.string());
        return false;
    }

    TypeDumper typeDumper {_db, typeIndexPath};
    if (!typeDumper.dump()) {
        spdlog::error("Error while dumping the database type index in {}",
                      _dbPath.string());
        return false;
    }

    EntityDumper entityDumper {_db, entityIndexPath};
    if (!entityDumper.dump()) {
        spdlog::error("Error while dumping the database entity index in {}",
                      _dbPath.string());
        return false;
    }

    return true;
}
