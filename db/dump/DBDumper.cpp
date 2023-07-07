#include "DBDumper.h"
#include "BioLog.h"
#include "DB.h"
#include "FileUtils.h"
#include "MsgCommon.h"
#include "MsgDB.h"
#include "StringIndexDumper.h"
#include "EntityDumper.h"
#include "TimerStat.h"
#include "TypeDumper.h"

using namespace db;
using namespace Log;

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

    BioLog::log(msg::INFO_DB_DUMPING_DATABASE() << _dbPath);

    // Remove DB dump directory if it already exists
    if (FileUtils::exists(_dbPath)) {
        if (!FileUtils::removeDirectory(_dbPath)) {
            BioLog::log(msg::ERROR_FAILED_TO_REMOVE_DIRECTORY() << _dbPath);
            return false;
        }
    }

    if (!FileUtils::createDirectory(_dbPath)) {
        BioLog::log(msg::ERROR_FAILED_TO_CREATE_DIRECTORY() << _dbPath);
        return false;
    }

    StringIndexDumper strDumper{stringIndexPath};
    if (!strDumper.dump(_db->_strIndex)) {
        BioLog::log(msg::ERROR_DB_DUMPING_DATABASE() << _dbPath);
        return false;
    }

    TypeDumper typeDumper {_db, typeIndexPath};
    if (!typeDumper.dump()) {
        BioLog::log(msg::ERROR_DB_DUMPING_DATABASE() << _dbPath);
        return false;
    }

    EntityDumper entityDumper {_db, entityIndexPath};
    if (!entityDumper.dump()) {
        BioLog::log(msg::ERROR_DB_DUMPING_DATABASE() << _dbPath);
        return false;
    }

    return true;
}
